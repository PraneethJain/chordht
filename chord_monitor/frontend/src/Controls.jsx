import React, { useState } from 'react';
import { addNode, putData, getData } from './api';

const Controls = ({ onLog }) => {
    const [isAdding, setIsAdding] = useState(false);
    const [putKey, setPutKey] = useState('');
    const [putValue, setPutValue] = useState('');
    const [getKey, setGetKey] = useState('');

    const handleAddNode = async () => {
        setIsAdding(true);
        onLog('Adding node...', 'info');
        try {
            const res = await addNode();
            if (res.data.success) {
                onLog(res.data.message, 'success');
            } else {
                onLog(res.data.message, 'error');
            }
        } catch (e) {
            onLog('Failed to add node: ' + e.message, 'error');
        } finally {
            setTimeout(() => setIsAdding(false), 2000);
        }
    };

    const handlePut = async () => {
        if (!putKey || !putValue) {
            onLog('Key and Value required', 'error');
            return;
        }
        onLog(`Putting ${putKey}=${putValue}...`, 'info');
        try {
            const res = await putData(putKey, putValue);
            if (res.data.success) {
                onLog(res.data.message, 'success');
            } else {
                onLog(res.data.message, 'error');
            }
        } catch (e) {
            onLog('Put failed: ' + e.message, 'error');
        }
    };

    const handleGet = async () => {
        if (!getKey) {
            onLog('Key required', 'error');
            return;
        }
        onLog(`Getting ${getKey}...`, 'info');
        try {
            const res = await getData(getKey);
            if (res.data.found) {
                const msg = `Found value: "${res.data.value}"`;
                onLog(msg, 'success');
                alert(msg);
            } else {
                const msg = `Key "${getKey}" not found`;
                onLog(msg, 'error');
                alert(msg);
            }
        } catch (e) {
            onLog('Get failed: ' + e.message, 'error');
        }
    };

    return (
        <div className="controls">
            <div className="control-group">
                <h3>Network Control</h3>
                <button onClick={handleAddNode} disabled={isAdding} className="btn primary">
                    {isAdding ? 'Adding...' : 'Add Node'}
                </button>
            </div>

            <div className="control-group">
                <h3>Put Data</h3>
                <input
                    type="text"
                    placeholder="Key"
                    value={putKey}
                    onChange={(e) => setPutKey(e.target.value)}
                />
                <input
                    type="text"
                    placeholder="Value"
                    value={putValue}
                    onChange={(e) => setPutValue(e.target.value)}
                />
                <button onClick={handlePut} className="btn secondary">Put</button>
            </div>

            <div className="control-group">
                <h3>Get Data</h3>
                <input
                    type="text"
                    placeholder="Key"
                    value={getKey}
                    onChange={(e) => setGetKey(e.target.value)}
                />
                <button onClick={handleGet} className="btn secondary">Get</button>
            </div>
        </div>
    );
};

export default Controls;
