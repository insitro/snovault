import React from 'react';
import globals from './globals';

const StatusLabel = React.createClass({
    propTypes: {
        status: React.PropTypes.oneOfType([
            React.PropTypes.string,
            React.PropTypes.array,
        ]).isRequired, // Array of status objects with status and badge title
        title: React.PropTypes.string,
        buttonLabel: React.PropTypes.string,
    },

    render: function () {
        const { status, title, buttonLabel } = this.props;

        if (typeof status === 'string') {
            // Display simple string and optional title in badge
            return (
                <ul className="status-list">
                    <li className={globals.statusClass(status, 'label')}>
                        {title ? <span className="status-list-title">{`${title}: `}</span> : null}
                        {buttonLabel || status}
                    </li>
                </ul>
            );
        } else if (typeof status === 'object') {
            // Display a list of badges from array of objects with status and optional title
            return (
                <ul className="status-list">
                    {status.map(singleStatus => (
                        <li key={singleStatus.title} className={globals.statusClass(singleStatus.status, 'label')}>
                            {singleStatus.title ? <span className="status-list-title">{`${singleStatus.title}: `}</span> : null}
                            {singleStatus.status}
                        </li>
                    ))}
                </ul>
            );
        }
        return null;
    },
});

export default StatusLabel;