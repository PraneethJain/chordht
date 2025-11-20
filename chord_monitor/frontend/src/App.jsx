import React, { useState, useEffect } from 'react';
import ChordRing from './ChordRing';
import Controls from './Controls';
import NodeDetailsModal from './NodeDetailsModal';
import { getState, leaveNode } from './api';
import './App.css';

function App() {
  const [nodes, setNodes] = useState([]);
  const [logs, setLogs] = useState([]);
  const [selectedNode, setSelectedNode] = useState(null);

  const addLog = (msg, type = 'info') => {
    setLogs(prev => [{ msg, type, time: new Date().toLocaleTimeString() }, ...prev]);
  };

  useEffect(() => {
    let active = true;

    const fetchNodes = async () => {
      try {
        const res = await getState();
        if (!active) return;

        setNodes(res.data);
        // Update selected node if it exists
        if (selectedNode) {
          const updated = res.data.find(n => n.id === selectedNode.id);
          if (updated) {
            setSelectedNode(updated);
          } else {
            // Node might have left
            setSelectedNode(null);
          }
        }
      } catch (e) {
        if (active) {
          console.error("Failed to fetch state", e);
        }
      }
    };

    fetchNodes();
    const interval = setInterval(fetchNodes, 1000);
    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [selectedNode]);

  const handleNodeClick = (node) => {
    setSelectedNode(node);
  };

  const handleCloseModal = () => {
    setSelectedNode(null);
  };

  const handleLeaveNode = async (id) => {
    if (confirm(`Are you sure you want node ${id} to leave the network?`)) {
      try {
        // Ensure ID is a string
        const idStr = id.toString();
        const res = await leaveNode(idStr);
        if (res.data.success) {
          addLog(`Node ${idStr} left the network`, 'success');
          setSelectedNode(null);
        } else {
          console.error(res.data.message);
          addLog(`Failed to leave node: ${res.data.message}`, 'error');
        }
      } catch (e) {
        console.error(e);
        addLog(`Failed to make node ${id} leave`, 'error');
      }
    }
  };

  return (
    <div className="app-container">
      <div className="sidebar">
        <h1>Chord DHT</h1>
        <Controls onLog={addLog} />

        <div className="node-list">
          <h3>Nodes ({nodes.length})</h3>
          {nodes.sort((a, b) => BigInt(a.id) < BigInt(b.id) ? -1 : 1).map(node => (
            <div
              key={node.id}
              className={`node-card ${selectedNode && selectedNode.id === node.id ? 'selected' : ''}`}
              onClick={() => handleNodeClick(node)}
              style={{ cursor: 'pointer' }}
            >
              <div className="node-id">{node.id.toString().substring(0, 16)}...</div>
              <div className="node-addr">{node.address}</div>
              <div className="node-keys">Keys: {node.stored_keys ? node.stored_keys.length : 0}</div>
            </div>
          ))}
        </div>

        <div className="log-panel">
          <h3>Logs</h3>
          <div className="logs">
            {logs.map((log, i) => (
              <div key={i} className={`log-entry log-${log.type}`}>
                [{log.time}] {log.msg}
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="main-view">
        <ChordRing nodes={nodes} />
      </div>

      {selectedNode && (
        <NodeDetailsModal
          node={selectedNode}
          onClose={handleCloseModal}
          onLeave={handleLeaveNode}
        />
      )}
    </div>
  );
}

export default App;
