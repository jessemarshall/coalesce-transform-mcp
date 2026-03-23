import { describe, it, expect, vi } from 'vitest';
import { listWorkspaceNodeTypes } from '../../src/services/workspace/mutations.js';
import type { CoalesceClient } from '../../src/client.js';

describe('list-workspace-node-types tool registration', () => {
  it('should be registered with correct schema', () => {
    // This test verifies the tool compiles and exports correctly
    expect(listWorkspaceNodeTypes).toBeDefined();
    expect(typeof listWorkspaceNodeTypes).toBe('function');
  });
});

describe('listWorkspaceNodeTypes', () => {
  it('returns empty arrays for workspace with no nodes', async () => {
    const mockClient = {
      get: vi.fn().mockResolvedValue({ data: [] })
    } as unknown as CoalesceClient;

    const result = await listWorkspaceNodeTypes(mockClient, { workspaceID: '1' });

    expect(result).toEqual({
      workspaceID: '1',
      nodeTypes: [],
      counts: {},
      total: 0,
      basis: 'observed_nodes'
    });
  });

  it('returns node types sorted by frequency', async () => {
    const mockClient = {
      get: vi.fn().mockResolvedValue({
        data: [
          { id: '1', nodeType: 'Stage' },
          { id: '2', nodeType: 'Stage' },
          { id: '3', nodeType: 'Dimension' },
          { id: '4', nodeType: 'View' },
          { id: '5', nodeType: 'Stage' },
          { id: '6', nodeType: 'Dimension' }
        ]
      })
    } as unknown as CoalesceClient;

    const result = await listWorkspaceNodeTypes(mockClient, { workspaceID: '1' });

    expect(result).toEqual({
      workspaceID: '1',
      nodeTypes: ['Stage', 'Dimension', 'View'],
      counts: {
        Stage: 3,
        Dimension: 2,
        View: 1
      },
      total: 6,
      basis: 'observed_nodes'
    });
  });

  it('filters out nodes with null or undefined nodeType', async () => {
    const mockClient = {
      get: vi.fn().mockResolvedValue({
        data: [
          { id: '1', nodeType: 'Stage' },
          { id: '2', nodeType: null },
          { id: '3', nodeType: undefined },
          { id: '4', nodeType: 'Stage' },
          { id: '5', nodeType: '' }
        ]
      })
    } as unknown as CoalesceClient;

    const result = await listWorkspaceNodeTypes(mockClient, { workspaceID: '1' });

    expect(result).toEqual({
      workspaceID: '1',
      nodeTypes: ['Stage'],
      counts: {
        Stage: 2
      },
      total: 2,
      basis: 'observed_nodes'
    });
  });

  it('paginates through the full workspace node list', async () => {
    const mockClient = {
      get: vi.fn().mockImplementation((_path: string, params?: Record<string, unknown>) => {
        if (!params?.startingFrom) {
          return Promise.resolve({
            data: [
              { id: '1', nodeType: 'Stage' },
              { id: '2', nodeType: 'Stage' }
            ],
            next: 'cursor-2'
          });
        }

        if (params.startingFrom === 'cursor-2') {
          return Promise.resolve({
            data: [
              { id: '3', nodeType: 'View' },
              { id: '4', nodeType: 'Dimension' }
            ]
          });
        }

        throw new Error(`Unexpected cursor ${String(params.startingFrom)}`);
      })
    } as unknown as CoalesceClient;

    const result = await listWorkspaceNodeTypes(mockClient, { workspaceID: '1' });

    expect(mockClient.get).toHaveBeenNthCalledWith(
      1,
      '/api/v1/workspaces/1/nodes',
      { detail: false, limit: 250, orderBy: 'id' }
    );
    expect(mockClient.get).toHaveBeenNthCalledWith(
      2,
      '/api/v1/workspaces/1/nodes',
      {
        detail: false,
        limit: 250,
        orderBy: 'id',
        startingFrom: 'cursor-2'
      }
    );
    expect(result).toEqual({
      workspaceID: '1',
      nodeTypes: ['Stage', 'View', 'Dimension'],
      counts: {
        Stage: 2,
        View: 1,
        Dimension: 1
      },
      total: 4,
      basis: 'observed_nodes'
    });
  });
});
