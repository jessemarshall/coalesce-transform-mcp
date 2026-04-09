// Stable fixtures extracted from the local Postman collection so tests do not
// depend on `Coalesce API.postman_collection.json` remaining in the repo.

export const POSTMAN_PROJECTS_QUERY = {
  includeWorkspaces: true,
  includeJobs: true,
} as const;

export const POSTMAN_USERS_QUERY = {
  limit: 50,
  startingFrom: "string",
  orderBy: "id",
} as const;

export const POSTMAN_USER_ROLES_QUERY = {
  projectID: "string",
  environmentID: "string",
} as const;

export const POSTMAN_START_RUN_RESPONSE = {
  runCounter: 0,
} as const;

export const POSTMAN_RERUN_RESPONSE = {
  runCounter: 0,
} as const;

export const POSTMAN_CANCEL_RUN_BODY = {
  runID: "<integer>",
  orgID: "<string>",
  environmentID: "<string>",
} as const;

export const POSTMAN_RUN_STATUS_RESPONSE = {
  runStatus: "<string>",
  runType: "<string>",
  runLink: "<string>",
} as const;

export const POSTMAN_RERUN_BODY = {
  runDetails: {
    runID: "<string>",
    forceIgnoreWorkspaceStatus: true,
  },
  userCredentials: {
    snowflakeUsername: "<string>",
    snowflakePassword: "<string>",
    snowflakeKeyPairKey: "<string>",
    snowflakeKeyPairPass: "<string>",
    snowflakeWarehouse: "<string>",
    snowflakeRole: "<string>",
    snowflakeAuthType: "Basic",
  },
  parameters: {},
} as const;

export const POSTMAN_RUN_DETAILS_RESPONSE = {
  id: "0",
  runDetails: {
    deployCommit: "<string>",
    environmentID: "<string>",
    canceled: "<boolean>",
    deployCommitMessage: "<string>",
    parallelism: "<integer>",
    nodesInRun: "<integer>",
  },
  runStatus: "canceled",
  runTimeParameters: {
    ut_8e: 40894307.86692253,
    aliqua_f4: 50908402.36900157,
  },
  runType: "deploy",
  reRunID: "<string>",
  runEndTime: "<dateTime>",
  runHistory: ["<integer>", "<integer>"],
  runStartTime: "<dateTime>",
  userCredentials: {
    snowflakeAccount: "<string>",
    snowflakeAuthType: "Basic",
    snowflakeUsername: "<string>",
    snowflakeRole: "<string>",
    snowflakeWarehouse: "<string>",
  },
  userID: "<string>",
  version: "<integer>",
} as const;
