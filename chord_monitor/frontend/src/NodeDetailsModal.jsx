import React from 'react';
import './NodeDetailsModal.css';

const NodeDetailsModal = ({ node, onClose, onLeave }) => {
    if (!node) return null;

    return (
        <div className="modal-overlay" onClick={onClose}>
            <div className="modal-content" onClick={e => e.stopPropagation()}>
                <div className="modal-header">
                    <h2>Node Details</h2>
                    <button className="close-button" onClick={onClose}>&times;</button>
                </div>
                <div className="modal-body">
                    <div className="detail-row">
                        <strong>ID:</strong> <span>{node.id}</span>
                    </div>
                    <div className="detail-row">
                        <strong>Address:</strong> <span>{node.address}</span>
                    </div>

                    <div className="section">
                        <h3>Predecessor</h3>
                        {node.predecessor ? (
                            <div className="detail-row">
                                <span>ID: {node.predecessor.id}</span>
                                <span>Addr: {node.predecessor.address}</span>
                            </div>
                        ) : (
                            <div>None</div>
                        )}
                    </div>

                    <div className="section">
                        <h3>Successors</h3>
                        <ul className="list">
                            {node.successors && node.successors.map((s, i) => (
                                <li key={i}>{s.id} ({s.address})</li>
                            ))}
                        </ul>
                    </div>

                    <div className="section">
                        <h3>Finger Table</h3>
                        <div className="table-container">
                            <table>
                                <thead>
                                    <tr>
                                        <th>Index</th>
                                        <th>Node ID</th>
                                        <th>Address</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {node.finger_table && node.finger_table.map((f, i) => (
                                        <tr key={i}>
                                            <td>{i}</td>
                                            <td>{f.id}</td>
                                            <td>{f.address}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>

                    <div className="section">
                        <h3>Stored Keys ({node.stored_keys ? node.stored_keys.length : 0})</h3>
                        <ul className="list scrollable">
                            {node.stored_keys && node.stored_keys.map((k, i) => (
                                <li key={i}>{k}</li>
                            ))}
                        </ul>
                    </div>
                </div>
                <div className="modal-footer">
                    <button className="leave-button" onClick={() => onLeave(node.id)}>
                        Leave Network
                    </button>
                </div>
            </div>
        </div>
    );
};

export default NodeDetailsModal;
