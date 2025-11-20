import React, { useEffect, useRef } from 'react';

const ChordRing = ({ nodes }) => {
    const canvasRef = useRef(null);

    const MAX_ID = 18446744073709551615n; // 2^64 - 1
    const RING_COLOR = '#444';
    const NODE_COLOR = '#00d2ff';
    const NODE_RADIUS = 12;
    const FONT_COLOR = '#e0e0e0';

    const mapIdToAngle = (id) => {
        const idBigInt = BigInt(id);
        const multiplier = 1000000000n;
        const scaled = (idBigInt * multiplier) / MAX_ID;
        const ratio = Number(scaled) / Number(multiplier);
        return ratio * 2 * Math.PI - Math.PI / 2;
    };

    useEffect(() => {
        const canvas = canvasRef.current;
        const ctx = canvas.getContext('2d');

        const resize = () => {
            const parent = canvas.parentElement;
            canvas.width = parent.clientWidth;
            canvas.height = parent.clientHeight;
            draw();
        };

        const draw = () => {
            const { width, height } = canvas;
            const centerX = width / 2;
            const centerY = height / 2;
            const radius = Math.min(centerX, centerY) * 0.8;

            ctx.clearRect(0, 0, width, height);

            if (nodes.length === 0) {
                ctx.fillStyle = '#666';
                ctx.font = '20px sans-serif';
                ctx.textAlign = 'center';
                ctx.fillText('Waiting for nodes...', centerX, centerY);
                return;
            }

            // Draw Ring
            ctx.beginPath();
            ctx.arc(centerX, centerY, radius, 0, 2 * Math.PI);
            ctx.strokeStyle = RING_COLOR;
            ctx.lineWidth = 4;
            ctx.stroke();

            // Draw Nodes
            nodes.forEach(node => {
                const angle = mapIdToAngle(node.id);
                const x = centerX + radius * Math.cos(angle);
                const y = centerY + radius * Math.sin(angle);

                // Node circle
                ctx.beginPath();
                ctx.arc(x, y, NODE_RADIUS, 0, 2 * Math.PI);
                ctx.fillStyle = NODE_COLOR;
                ctx.shadowColor = NODE_COLOR;
                ctx.shadowBlur = 10;
                ctx.fill();
                ctx.shadowBlur = 0;

                // ID Label
                ctx.fillStyle = FONT_COLOR;
                ctx.font = '12px monospace';
                ctx.textAlign = 'left';
                ctx.fillText(node.id.toString().substring(0, 6), x + 15, y + 4);

                // Key Count Label (if keys exist)
                if (node.stored_keys && node.stored_keys.length > 0) {
                    ctx.fillStyle = '#ffcc00';
                    ctx.font = '10px monospace';
                    ctx.fillText(`${node.stored_keys.length} keys`, x + 15, y + 16);
                }
            });
        };

        window.addEventListener('resize', resize);
        resize();

        return () => window.removeEventListener('resize', resize);
    }, [nodes]);

    return (
        <div style={{ width: '100%', height: '100%', minHeight: '400px' }}>
            <canvas
                ref={canvasRef}
            />
        </div>
    );
};

export default ChordRing;
