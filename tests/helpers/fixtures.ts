import { vi } from "vitest";

export function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

export function buildSourceColumn(name: string, nodeID: string, columnID: string) {
  return {
    name,
    columnID,
    dataType: "VARCHAR",
    nullable: true,
    columnReference: {
      stepCounter: nodeID,
      columnCounter: columnID,
    },
    sources: [
      {
        columnReferences: [
          {
            nodeID,
            columnID,
          },
        ],
      },
    ],
  };
}

export function buildSourceNode(nodeID: string, name: string, locationName: string | null = "RAW") {
  return {
    id: nodeID,
    name,
    ...(locationName ? { locationName } : {}),
    metadata: {
      columns: [
        buildSourceColumn("CUSTOMER_ID", nodeID, `${nodeID}-cust-id`),
        buildSourceColumn("CUSTOMER_NAME", nodeID, `${nodeID}-cust-name`),
      ],
    },
  };
}

export function buildCreatedStageNode(predecessorNodeID: string) {
  return {
    id: "new-node",
    name: "STG_CUSTOMER",
    description: "",
    locationName: "STAGING",
    database: "STAGING",
    schema: "ANALYTICS",
    config: {
      preSQL: "",
      postSQL: "",
      testsEnabled: true,
    },
    metadata: {
      columns: [
        {
          name: "CUSTOMER_ID",
          dataType: "NUMBER(38,0)",
          nullable: false,
          columnReference: {
            stepCounter: "new-node",
            columnCounter: "new-customer-id",
          },
          sources: [
            {
              columnReferences: [
                {
                  nodeID: predecessorNodeID,
                  columnID: `${predecessorNodeID}-cust-id`,
                },
              ],
            },
          ],
        },
        {
          name: "CUSTOMER_NAME",
          dataType: "VARCHAR(256)",
          nullable: true,
          columnReference: {
            stepCounter: "new-node",
            columnCounter: "new-customer-name",
          },
          sources: [
            {
              columnReferences: [
                {
                  nodeID: predecessorNodeID,
                  columnID: `${predecessorNodeID}-cust-name`,
                },
              ],
            },
          ],
        },
      ],
      sourceMapping: [
        {
          aliases: {},
          customSQL: { customSQL: "" },
          dependencies: [
            {
              locationName: "RAW",
              nodeName: "CUSTOMER",
            },
          ],
          join: {
            joinCondition: `FROM {{ ref('RAW', 'CUSTOMER') }} "CUSTOMER"`,
          },
          name: "STG_CUSTOMER",
          noLinkRefs: [],
        },
      ],
    },
  };
}
