import { useState, useEffect } from 'react';
import axios from 'axios';

interface FileEntry {
    name: string;
    is_dir: boolean;
}

interface DirListing {
    path: string;
    parent: string | null;
    entries: FileEntry[];
}

interface Props {
    initialPath: string;
    restartOnSave: boolean;
    onConfirm: (path: string) => void;
    onCancel: () => void;
}

const btnBase: React.CSSProperties = {
    padding: '7px 18px',
    borderRadius: '4px',
    cursor: 'pointer',
    fontSize: '13px',
    fontFamily: 'monospace',
    fontWeight: 'bold',
};

export function FileBrowserDialog({ initialPath, restartOnSave, onConfirm, onCancel }: Props) {
    const [listing, setListing] = useState<DirListing | null>(null);
    const [filename, setFilename] = useState(initialPath);
    const [loadError, setLoadError] = useState<string | null>(null);
    const [selectedEntry, setSelectedEntry] = useState<string | null>(null);

    const loadDir = (path: string) => {
        setLoadError(null);
        axios.get<DirListing>('/api/files', { params: { path } })
            .then(r => setListing(r.data))
            .catch(e => setLoadError(e.response?.data || e.message));
    };

    useEffect(() => {
        // Navigate to the directory containing the current config file
        const lastSlash = initialPath.lastIndexOf('/');
        const dir = lastSlash > 0 ? initialPath.substring(0, lastSlash) : '.';
        loadDir(dir);
        setFilename(initialPath);
    }, [initialPath]);

    const handleEntry = (entry: FileEntry) => {
        if (entry.is_dir) {
            const newPath = (listing!.path.endsWith('/') ? listing!.path : listing!.path + '/') + entry.name;
            loadDir(newPath);
            setSelectedEntry(null);
        } else {
            const fullPath = (listing!.path.endsWith('/') ? listing!.path : listing!.path + '/') + entry.name;
            setFilename(fullPath);
            setSelectedEntry(entry.name);
        }
    };

    const handleConfirm = () => {
        const trimmed = filename.trim();
        if (trimmed) onConfirm(trimmed);
    };

    return (
        <div style={{
            position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
            background: 'rgba(0,0,0,0.72)', zIndex: 1000,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
            <div style={{
                background: '#0f172a',
                border: '1px solid #334155',
                borderRadius: '10px',
                padding: '24px',
                width: '520px',
                maxHeight: '72vh',
                display: 'flex',
                flexDirection: 'column',
                color: '#f8fafc',
                fontFamily: 'monospace',
                boxShadow: '0 20px 60px rgba(0,0,0,0.6)',
            }}>
                <h3 style={{ margin: '0 0 14px', color: '#60a5fa', fontSize: '15px', fontWeight: 'bold' }}>
                    Save Configuration — Select Target File
                </h3>

                {/* Current directory path */}
                <div style={{
                    fontSize: '11px', color: '#94a3b8', marginBottom: '6px',
                    wordBreak: 'break-all', padding: '4px 6px',
                    background: '#1e293b', borderRadius: '4px',
                }}>
                    {listing?.path ?? '…'}
                </div>

                {/* Parent directory button */}
                {listing?.parent != null && (
                    <button
                        onClick={() => loadDir(listing.parent!)}
                        style={{
                            ...btnBase, textAlign: 'left', background: 'transparent',
                            color: '#f59e0b', border: '1px solid #334155',
                            marginBottom: '6px', padding: '5px 10px',
                        }}
                    >
                        ↑ .. (parent directory)
                    </button>
                )}

                {/* File / directory listing */}
                <div style={{
                    flex: 1, overflowY: 'auto',
                    border: '1px solid #334155', borderRadius: '4px',
                    marginBottom: '12px', minHeight: '160px',
                    background: '#1e293b',
                }}>
                    {loadError && (
                        <div style={{ color: '#ef4444', padding: '8px 10px', fontSize: '12px' }}>
                            {loadError}
                        </div>
                    )}
                    {listing?.entries.map(entry => (
                        <div
                            key={entry.name}
                            onClick={() => handleEntry(entry)}
                            style={{
                                padding: '5px 10px',
                                cursor: 'pointer',
                                fontSize: '13px',
                                color: entry.is_dir
                                    ? '#60a5fa'
                                    : entry.name.endsWith('.toml')
                                        ? '#4ade80'
                                        : '#cbd5e1',
                                background: selectedEntry === entry.name ? '#334155' : 'transparent',
                                borderBottom: '1px solid #0f172a',
                                userSelect: 'none',
                            }}
                            onMouseEnter={ev => {
                                if (selectedEntry !== entry.name)
                                    ev.currentTarget.style.background = '#1e3a5f';
                            }}
                            onMouseLeave={ev => {
                                ev.currentTarget.style.background =
                                    selectedEntry === entry.name ? '#334155' : 'transparent';
                            }}
                        >
                            {entry.is_dir ? '📁 ' : '📄 '}{entry.name}
                        </div>
                    ))}
                    {listing && listing.entries.length === 0 && !loadError && (
                        <div style={{ color: '#64748b', padding: '8px 10px', fontSize: '12px' }}>
                            (empty directory)
                        </div>
                    )}
                </div>

                {/* Editable filename / path */}
                <label style={{ fontSize: '11px', color: '#94a3b8', marginBottom: '4px' }}>
                    Save path (edit or type new filename):
                </label>
                <input
                    type="text"
                    value={filename}
                    onChange={e => setFilename(e.target.value)}
                    onKeyDown={e => { if (e.key === 'Enter') handleConfirm(); if (e.key === 'Escape') onCancel(); }}
                    style={{
                        padding: '8px 10px',
                        background: '#1e293b',
                        border: '1px solid #475569',
                        borderRadius: '4px',
                        color: '#f8fafc',
                        fontFamily: 'monospace',
                        fontSize: '13px',
                        marginBottom: '16px',
                        outline: 'none',
                    }}
                    autoFocus
                />

                {/* Action buttons */}
                <div style={{ display: 'flex', gap: '10px', justifyContent: 'flex-end' }}>
                    <button
                        onClick={onCancel}
                        style={{ ...btnBase, background: 'transparent', color: '#94a3b8', border: '1px solid #475569' }}
                    >
                        Cancel
                    </button>
                    <button
                        onClick={handleConfirm}
                        disabled={!filename.trim()}
                        style={{
                            ...btnBase,
                            background: filename.trim()
                                ? (restartOnSave ? '#ef4444' : '#2563eb')
                                : '#1e293b',
                            color: 'white',
                            border: 'none',
                            opacity: filename.trim() ? 1 : 0.5,
                        }}
                    >
                        {restartOnSave ? '🔄 Save & Restart' : '💾 Save to Disk'}
                    </button>
                </div>
            </div>
        </div>
    );
}
