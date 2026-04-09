export interface NodeContext {
  hasMultipleSources: boolean;
  hasAggregates: boolean;
  hasTimestampColumns: boolean;
  hasType2Pattern: boolean;
  materializationType: "table" | "view";
  columnPatterns: {
    timestamps: string[];
    dates: string[];
    businessKeys: string[];
    changeTrackingCandidates: string[];
  };
}

interface ColumnLike {
  name?: unknown;
  transform?: unknown;
  isBusinessKey?: unknown;
}

interface SourceMappingLike {
  dependencies?: unknown[];
}

export function analyzeNodeContext(node: Record<string, unknown>): NodeContext {
  const metadata = (node.metadata ?? {}) as Record<string, unknown>;
  const sourceMapping = Array.isArray(metadata.sourceMapping)
    ? (metadata.sourceMapping as SourceMappingLike[])
    : [];
  const columns = Array.isArray(metadata.columns)
    ? (metadata.columns as ColumnLike[])
    : [];

  // Detect multiple sources from sourceMapping dependencies
  const totalDependencies = sourceMapping.reduce((count: number, mapping) => {
    return count + (Array.isArray(mapping?.dependencies) ? mapping.dependencies.length : 0);
  }, 0);

  // Detect aggregates
  const aggregatePattern = /\b(COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE|LISTAGG|ARRAY_AGG)\s*\(/i;
  const hasAggregates = columns.some((col) =>
    aggregatePattern.test(typeof col.transform === "string" ? col.transform : "")
  );

  // Detect timestamp columns
  const timestampPattern = /_TS$|_TIMESTAMP$|TIMESTAMP_/i;
  const datePattern = /_DATE$|_DT$|DATE_/i;

  const colName = (col: ColumnLike): string =>
    typeof col.name === "string" ? col.name : "";

  const timestamps = columns
    .filter((col) => timestampPattern.test(colName(col)))
    .map(colName);

  const dates = columns
    .filter((col) => datePattern.test(colName(col)))
    .map(colName);

  const hasTimestampColumns = timestamps.length > 0 || dates.length > 0;

  // Detect Type 2 SCD pattern (START_DATE, END_DATE, IS_CURRENT)
  const hasStartDate = columns.some((col) =>
    /START_DATE|EFFECTIVE_DATE/i.test(colName(col))
  );
  const hasEndDate = columns.some((col) =>
    /END_DATE|EXPIRY_DATE/i.test(colName(col))
  );
  const hasCurrentFlag = columns.some((col) =>
    /IS_CURRENT|CURRENT_FLAG/i.test(colName(col))
  );

  const hasType2Pattern = hasStartDate && hasEndDate && hasCurrentFlag;

  // Detect business key candidates: columns with ID/KEY/CODE patterns
  const businessKeys = columns
    .filter((col) => {
      const name = colName(col);
      return /_(ID|KEY|CODE|NUM)$/i.test(name) || /^(ID|KEY|CODE)_/i.test(name);
    })
    .map(colName);

  // Detect change tracking candidates: non-key, non-aggregate, non-system columns
  // These are columns that represent mutable business data
  const systemColumnPattern = /^(SYS_|DW_|ETL_|LOAD_|CREATED_|UPDATED_|MODIFIED_|INSERT_|UPDATE_)/i;
  const businessKeySet = new Set(businessKeys);
  const changeTrackingCandidates = columns
    .filter((col) => {
      const name = colName(col);
      if (businessKeySet.has(name)) return false;
      if (systemColumnPattern.test(name)) return false;
      if (aggregatePattern.test(typeof col.transform === "string" ? col.transform : "")) return false;
      if (col.isBusinessKey === true) return false;
      return name.length > 0;
    })
    .map(colName);

  // Read actual materialization type from node config
  const config = (node.config ?? {}) as Record<string, unknown>;
  const rawMaterialization = typeof config.materializationType === "string"
    ? config.materializationType.toLowerCase()
    : "";
  const materializationType: "table" | "view" =
    rawMaterialization.includes("view") ? "view" : "table";

  return {
    hasMultipleSources: totalDependencies > 1,
    hasAggregates,
    hasTimestampColumns,
    hasType2Pattern,
    materializationType,
    columnPatterns: {
      timestamps,
      dates,
      businessKeys,
      changeTrackingCandidates,
    },
  };
}
