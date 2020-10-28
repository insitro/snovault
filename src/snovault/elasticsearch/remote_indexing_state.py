AWS_REGION = 'us-west-2'
HEAD_NODE_INDEX = 'head_node'
INDEXING_NODE_INDEX = 'indexing_node'
_HOSTNAME = socket.gethostname()
# Allow three minutes for indexer to stop before head indexer process continues
_REMOTE_INDEXING_SHUTDOWN_SLEEP = 3*60
# Allow indexer to remain up for 30 minutes after indexing before being shutdown
_REMOTE_INDEXING_SHUTDOWN = 15*60
# Allow indexer 20 minutes to startup before head indexer thread takes over indexing
_REMOTE_INDEXING_STARTUP = 10*60
# The number of uuids needed to start a remote indexer
_REMOTE_INDEXING_THRESHOLD = 10000


def _get_this_instance_name(instance_name=None):
    ec2 = boto3.client('ec2', region_name=AWS_REGION)
    if not instance_name:
        hostname = _HOSTNAME.replace('ip-', '').replace('-', '.')
        response = ec2.describe_instances(Filters=[
                {'Name': 'private-ip-address', 'Values': [hostname]},
        ])
    else:
        response = ec2.describe_instances(Filters=[
            {'Name': 'tag:Name', 'Values': [instance_name]},
        ])
    if not response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
        return None 
    instance_name = None
    instance_state = None
    for reservation in response.get('Reservations', []):
        for instance in reservation.get('Instances', []):
            instance_state = instance['State']['Name']
            for tag_obj in instance.get('Tags', []):
                if tag_obj.get('Key') == 'Name':
                    instance_name = tag_obj.get('Value')
                    break
    return instance_name, instance_state


def determine_indexing_protocol(request, uuid_count):
    remote_indexing = asbool(request.registry.settings.get('remote_indexing', False))
    if not remote_indexing:
        return False
    try:
        indexer_short_uuids = int(request.registry.settings.get('indexer_short_uuids', 0))
        remote_indexing_threshold = int(
            request.registry.settings.get(
                'remote_indexing_threshold',
                _REMOTE_INDEXING_THRESHOLD,
            )
        )
        if indexer_short_uuids > 0:
            uuid_count = indexer_short_uuids
    except ValueError:
        log.warning('ValueError casting remote indexing threshold to an int')
        remote_indexing_threshold = _REMOTE_INDEXING_THRESHOLD
    if uuid_count < remote_indexing_threshold:
        return False
    return True


def get_nodes(request, indexer_state):
    label = ''
    continue_on = False
    did_timeout = False
    # Get local/remote indexing state
    head_node, indexing_node, time_now, did_fail, is_indexing_node = setup_indexing_nodes(
        request, indexer_state
    )
    this_node = head_node
    other_node = indexing_node
    if is_indexing_node:
        this_node = indexing_node
        other_node = head_node
    if did_fail:
        did_timeout = True
        return this_node, other_node, continue_on, did_timeout
    this_time_in_state = time_now - float(this_node['last_run_time'])
    other_time_in_state = time_now - float(other_node['last_run_time'])
    # Check for early return if indexing is running
    if this_node['node_index'] == HEAD_NODE_INDEX:
        if other_node['done_indexing']:
            label = 'Head node with remote finished indexing.  Reset both es node states.'
            this_node, other_node, time_now, did_fail, is_indexing_node = setup_indexing_nodes(
                request,
                indexer_state,
                reset=True
            )
        elif other_node['started_indexing']:
            # not catching if instance is on but app not running?  maybe the indexer state
            #  needs update a flag saying it is still running
            if other_node['instance_state'] in ['running', 'pending']:
                # label = 'Head node with remote indexing running'
                pass
            else:
                label = 'Head node with remote indexing running, permature shutdown'
                log.warning(f"Indexer permature shutdown: sleep {_REMOTE_INDEXING_SHUTDOWN_SLEEP} seconds")
                time.sleep(_REMOTE_INDEXING_SHUTDOWN_SLEEP)
                continue_on = True
        elif this_node['waiting_on_remote']:
            label = 'Head node waiting on remote indexing to start'
            if this_time_in_state > _REMOTE_INDEXING_STARTUP:
                this_node, other_node, time_now, did_fail, is_indexing_node = setup_indexing_nodes(
                    request,
                    indexer_state,
                    reset=True
                )
                if not other_node['instance_state'] in ['stopped', 'stopping']:
                    instance_name = this_node['instance_name']
                    remote_indexing = False
                    _ = _send_sqs_msg(
                        f"{instance_name}-indexer",
                        remote_indexing,
                        'Turn OFF remote indexer for failed start',
                        delay_seconds=0,
                        save_event=False
                    )
                    log.warning(f"Indexer failed start shutdown sleep: {_REMOTE_INDEXING_SHUTDOWN_SLEEP} seconds")
                    time.sleep(_REMOTE_INDEXING_SHUTDOWN_SLEEP)
                else:
                    # log.warning('Indexer failed start shutdown: Already stopped')
                    pass
                did_timeout = True
        else:
            # label = 'Head node says no indexing going on'
            if this_time_in_state > _REMOTE_INDEXING_SHUTDOWN:
                this_node, other_node, time_now, did_fail, is_indexing_node = setup_indexing_nodes(
                    request,
                    indexer_state,
                    reset=True
                )
                if not other_node['instance_state'] in ['stopped', 'stopping']:
                    label = 'Head node says no indexing going on - and shutting down indexer'
                    instance_name = this_node['instance_name']
                    remote_indexing = False
                    _ = _send_sqs_msg(
                        f"{instance_name}-indexer",
                        remote_indexing,
                        'Turn OFF remote indexer for shutdown',
                        delay_seconds=0,
                        save_event=False
                    )
                    log.warning(f"Indexer timeout shutdown: sleep {_REMOTE_INDEXING_SHUTDOWN_SLEEP} seconds")
                    time.sleep(_REMOTE_INDEXING_SHUTDOWN_SLEEP)
                else:
                    # log.warning('Indexer timeout shutdown: Already stopped')
                    pass
            continue_on = True
    elif this_node['node_index'] == INDEXING_NODE_INDEX:
        if other_node['waiting_on_remote']:
            if not this_node['started_indexing']:
                label = 'Index node received signal to start indexing'
                continue_on = True
            elif this_node['done_indexing']:
                label = 'Index node finished indexing.  Waiting on head node to pick up status.'
            elif this_node['started_indexing']:
                label = 'Index node has just finished indexing'
                this_node['done_indexing'] = True
                this_node['last_run_time'] = str(float(time.time()))
                indexer_state.put_obj(this_node['node_index'], this_node)
            else:
                label = 'Index node is in limbo with head node waiting_on_remote.  May correct on next loop.'
        else:
            label = 'Index node finished indexing and has been reset by head node.  Waiting to be shutdown by head node.'
    if label:
        if other_node['instance_state'] == 'stopped':
            log.warning(f"Remote indexing _get_nodes: {label}. this_time: {this_time_in_state:0.6f}, other_time: stopped")
        else:
            log.warning(f"Remote indexing _get_nodes: {label}. this_time: {this_time_in_state:0.6f}, other_time: {other_time_in_state:0.6f}")
    return this_node, other_node, continue_on, did_timeout


def send_sqs_msg(
            cluster_name,
            remote_indexing,
            body,
            delay_seconds=0,
            save_event=False,
        ):
    did_fail = True
    msg_attrs = {
        'cluster_name': cluster_name,
        'remote_indexing': '0' if remote_indexing else '1',
        'save_event': '0' if save_event else '1',
    }
    msg_attrs = {
        key: {'DataType': 'String', 'StringValue': val}
        for key, val in msg_attrs.items()
    }
    boto_session = boto3.Session(region_name=AWS_REGION)
    sqs_resource = boto_session.resource('sqs')
    watch_dog = 3
    attempts = 0
    while attempts < watch_dog:
        attempts += 1
        try: 
            queue_arn = sqs_resource.get_queue_by_name(QueueName='ec2_scheduler_input')
            if queue_arn:
                response = sqs_resource.meta.client.send_message(
                    QueueUrl=queue_arn.url,
                    DelaySeconds=delay_seconds,
                    MessageAttributes=msg_attrs,
                    MessageBody=body,
                )
                if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
                    log.warning(f"Remote indexing _send_sqs_msg: {body}")
                    did_fail = False
                    break
        except ClientError as ecp:
            error_str = e.response.get('Error', {}).get('Code', 'Unknown')
            log.warning(f"({attempts} of {watch_dog}) Indexer could not send message to queue: {error_str}")
    return did_fail


def setup_indexing_nodes(request, indexer_state, reset=False):
    time_now = float(time.time())
    did_fail = False
    is_indexing_node = False
    node_state_template = {
        'node_index': None,
        'instance_name': None,
        'instance_state': None,
        'waiting_on_remote': False,
        'started_indexing': False,
        'done_indexing': False,
        'last_run_time': str(time_now),
    }
    # Get/Setup head node state in local elasticsearch meta data
    head_node = indexer_state.get_obj(HEAD_NODE_INDEX)
    indexing_node = indexer_state.get_obj(INDEXING_NODE_INDEX)
    indexer_tag = '-indexer'
    # Check which node this is
    instance_name, instance_state = _get_this_instance_name()
    if not instance_name:
        did_fail = True
        return head_node, indexing_node, time_now, did_fail, is_indexing_node
    elif not instance_name.replace(indexer_tag, '') == instance_name:
        # indexing node does not manage states
        is_indexing_node = True
        return head_node, indexing_node, time_now, did_fail, is_indexing_node
    # Handle Setup or Refresh
    if not head_node or not indexing_node or reset:
        # Setup node state in elasticsearch
        # Save this node as head node
        head_node = copy.deepcopy(node_state_template)
        head_node['node_index'] = HEAD_NODE_INDEX
        head_node['instance_name'] = instance_name
        head_node['instance_state'] = instance_state
        indexer_state.put_obj(head_node['node_index'], head_node)
        # Create indexing node
        indexer_name = f"{instance_name}{indexer_tag}"
        indexer_instance_name, indexer_instance_state = _get_this_instance_name(
            instance_name=indexer_name
        )
        if not indexer_instance_name:
            did_fail = True
            return head_node, indexing_node, time_now, did_fail, is_indexing_node
        indexing_node = copy.deepcopy(node_state_template)
        indexing_node['node_index'] = INDEXING_NODE_INDEX
        indexing_node['instance_name'] = indexer_instance_name
        indexing_node['instance_state'] = indexer_instance_state
        indexing_node['last_run_time'] = str(float(time.time()))
        indexer_state.put_obj(indexing_node['node_index'], indexing_node)
    else:
        # Refresh aws instance state
        # Head
        instance_name, instance_state = _get_this_instance_name(
            instance_name=head_node['instance_name']
        )
        if instance_name and instance_state:
            head_node['instance_state'] = instance_state
            indexer_state.put_obj(head_node['node_index'], head_node)
        else:
            log.warning('Remote indexer failed to update head node')
        # Indexer
        instance_name, instance_state = _get_this_instance_name(
            instance_name=indexing_node['instance_name']
        )
        if instance_name and instance_state:
            indexing_node['instance_state'] = instance_state
            if instance_state == 'stopped':
                # reset clock if stopped
                indexing_node['last_run_time'] = str(float(time.time()))
            indexer_state.put_obj(indexing_node['node_index'], indexing_node)
        else:
            log.warning('Remote indexer failed to update indexer node')
    return head_node, indexing_node, time_now, did_fail, is_indexing_node
