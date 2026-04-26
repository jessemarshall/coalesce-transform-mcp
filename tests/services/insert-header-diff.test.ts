import { describe, expect, it } from "vitest";
import {
	diffInsertHeader,
	extractInsertHeader,
} from "../../src/services/templates/insert-header-diff.js";

const SAMPLE_DML = `INSERT INTO "DB"."LOC"."NAME" (
    "COL1",
    "COL2",
    "COL3"
) (
    SELECT a AS "COL1", b AS "COL2", c AS "COL3" FROM "DB"."LOC"."SRC" SRC
);`;

// ── extractInsertHeader ─────────────────────────────────────────────────────

describe("extractInsertHeader", () => {
	it("extracts the three-part target identifier", () => {
		const result = extractInsertHeader(SAMPLE_DML);
		expect(result?.kind).toBe("ok");
		if (result?.kind === "ok") {
			expect(result.target).toEqual({
				database: "DB",
				locationName: "LOC",
				name: "NAME",
			});
		}
	});

	it("extracts the column list in order", () => {
		const result = extractInsertHeader(SAMPLE_DML);
		if (result?.kind === "ok") {
			expect(result.columns).toEqual(["COL1", "COL2", "COL3"]);
		}
	});

	it("returns null for non-INSERT SQL", () => {
		expect(extractInsertHeader(`SELECT * FROM T`)).toBeNull();
		expect(extractInsertHeader(`CREATE TABLE FOO (x NUMBER)`)).toBeNull();
	});

	it("ignores leading whitespace and comments before INSERT", () => {
		const sql = `-- a comment\n   /* block */ INSERT INTO "DB"."LOC"."NAME" (a) (SELECT a FROM x)`;
		const result = extractInsertHeader(sql);
		expect(result?.kind).toBe("ok");
		if (result?.kind === "ok") {
			expect(result.target.name).toBe("NAME");
		}
	});

	// ── Pass-1 fixes: malformed-INSERT detection ────────────────────────────

	it("returns malformed for INSERT with a two-part identifier (was: silent notApplicable)", () => {
		const result = extractInsertHeader(
			`INSERT INTO "LOC"."NAME" (a) (SELECT a FROM x)`,
		);
		expect(result?.kind).toBe("malformed");
		if (result?.kind === "malformed") {
			expect(result.reason).toMatch(/three-part identifier/);
		}
	});

	it("returns malformed for INSERT missing the column list", () => {
		const result = extractInsertHeader(
			`INSERT INTO "DB"."LOC"."NAME" SELECT a FROM x`,
		);
		expect(result?.kind).toBe("malformed");
	});
});

// ── diffInsertHeader ────────────────────────────────────────────────────────

describe("diffInsertHeader", () => {
	const node = { database: "DB", locationName: "LOC", name: "NAME" };

	it("identical when target and columns match", () => {
		const result = diffInsertHeader(SAMPLE_DML, node, ["COL1", "COL2", "COL3"]);
		expect(result).toEqual({ kind: "identical" });
	});

	it("targetChanged when the table name differs", () => {
		const renamedSql = SAMPLE_DML.replace(`"NAME"`, `"NEW_NAME"`);
		const result = diffInsertHeader(renamedSql, node, ["COL1", "COL2", "COL3"]);
		expect(result.kind).toBe("targetChanged");
		if (result.kind === "targetChanged") {
			expect(result.changedFields).toEqual(["name"]);
			expect(result.to.name).toBe("NEW_NAME");
		}
	});

	it("targetChanged when the location differs", () => {
		const relocSql = SAMPLE_DML.replace(`"LOC"`, `"NEW_LOC"`);
		const result = diffInsertHeader(relocSql, node, ["COL1", "COL2", "COL3"]);
		expect(result.kind).toBe("targetChanged");
		if (result.kind === "targetChanged") {
			expect(result.changedFields).toContain("locationName");
		}
	});

	it("targetChanged with multiple fields when multiple parts differ", () => {
		const dbAndNameSql = SAMPLE_DML
			.replace(`"DB"`, `"NEW_DB"`)
			.replace(`"NAME"`, `"NEW_NAME"`);
		const result = diffInsertHeader(dbAndNameSql, node, ["COL1", "COL2", "COL3"]);
		if (result.kind === "targetChanged") {
			expect(result.changedFields.sort()).toEqual(["database", "name"]);
		}
	});

	it("columnListMismatch when INSERT has different column count than SELECT", () => {
		const result = diffInsertHeader(SAMPLE_DML, node, ["COL1", "COL2"]);
		expect(result.kind).toBe("columnListMismatch");
		if (result.kind === "columnListMismatch") {
			expect(result.reason).toMatch(/3 columns but SELECT projects 2/);
		}
	});

	it("columnListMismatch when INSERT has a different column name at the same position", () => {
		const mismatchSql = SAMPLE_DML.replace(`"COL2"`, `"OTHER"`);
		const result = diffInsertHeader(mismatchSql, node, ["COL1", "COL2", "COL3"]);
		expect(result.kind).toBe("columnListMismatch");
		if (result.kind === "columnListMismatch") {
			expect(result.reason).toMatch(/position 2.*OTHER/);
		}
	});

	it("notApplicable for non-INSERT SQL (lets DDL inputs flow through)", () => {
		const result = diffInsertHeader(
			`CREATE TABLE FOO (x NUMBER)`,
			node,
			["X"],
		);
		expect(result).toEqual({ kind: "notApplicable" });
	});

	it("compares column names case-insensitively", () => {
		const lowercaseSql = SAMPLE_DML.replace(/"COL([123])"/g, '"col$1"');
		const result = diffInsertHeader(lowercaseSql, node, ["COL1", "COL2", "COL3"]);
		expect(result).toEqual({ kind: "identical" });
	});

	// ── Pass-1 fixes: case-folded identifier comparison ────────────────────

	it("treats `\"DB\".\"LOC\".\"NAME\"` as identical to `db.loc.name` (Snowflake folding)", () => {
		const lowercaseSql = SAMPLE_DML.replace(/"DB"\."LOC"\."NAME"/, "db.loc.name");
		const result = diffInsertHeader(lowercaseSql, node, ["COL1", "COL2", "COL3"]);
		expect(result).toEqual({ kind: "identical" });
	});

	// ── Pass-1 review: malformed-header surfaces via diffInsertHeader ──────

	it("surfaces a malformedHeader diff (not notApplicable) when INSERT is two-part", () => {
		const sql = `INSERT INTO "LOC"."NAME" (COL1) (SELECT a FROM x)`;
		const result = diffInsertHeader(sql, node, ["COL1"]);
		expect(result.kind).toBe("malformedHeader");
	});

	// ── Pass-1 review: locked-in contracts ──────────────────────────────────

	it("rejects when columns are reordered between INSERT and SELECT", () => {
		const reorderedSql = SAMPLE_DML.replace(
			`"COL1",\n    "COL2",\n    "COL3"`,
			`"COL2",\n    "COL1",\n    "COL3"`,
		);
		const result = diffInsertHeader(reorderedSql, node, ["COL1", "COL2", "COL3"]);
		expect(result.kind).toBe("columnListMismatch");
		if (result.kind === "columnListMismatch") {
			expect(result.reason).toMatch(/position 1.*COL2/);
		}
	});

	it("supports `$` in identifiers (Snowflake-legal)", () => {
		const sql = `INSERT INTO "DB"."LOC"."NAME$2024" ("ITEM$ID") (SELECT a FROM x)`;
		const result = diffInsertHeader(sql, { ...node, name: "NAME$2024" }, ["ITEM$ID"]);
		expect(result).toEqual({ kind: "identical" });
	});

	it("preserves existing case for unchanged fields when only one part differs", () => {
		// User types `db.loc.NEW_NAME` (lowercase bare for db/loc, only
		// renaming the table). Existing has upper-case `DB`/`LOC`/`NAME`.
		// `to.database` and `to.locationName` should preserve the
		// existing upper-case (so the cloud body keeps Coalesce's
		// canonical form); only `to.name` reflects the user's edit.
		const renameSql = `INSERT INTO db.loc."NEW_NAME" ("COL1", "COL2", "COL3") (SELECT a FROM x)`;
		const result = diffInsertHeader(renameSql, node, ["COL1", "COL2", "COL3"]);
		expect(result.kind).toBe("targetChanged");
		if (result.kind === "targetChanged") {
			expect(result.changedFields).toEqual(["name"]);
			expect(result.to).toEqual({
				database: "DB",       // preserved from existing (not user's "db")
				locationName: "LOC",  // preserved from existing
				name: "NEW_NAME",     // updated from user's edit
			});
		}
	});

	it("targetChanged with all three fields differing", () => {
		const allChangedSql = SAMPLE_DML
			.replace(`"DB"`, `"NEW_DB"`)
			.replace(`"LOC"`, `"NEW_LOC"`)
			.replace(`"NAME"`, `"NEW_NAME"`);
		const result = diffInsertHeader(allChangedSql, node, ["COL1", "COL2", "COL3"]);
		if (result.kind === "targetChanged") {
			expect(result.changedFields.sort()).toEqual(["database", "locationName", "name"]);
			expect(result.to).toEqual({
				database: "NEW_DB",
				locationName: "NEW_LOC",
				name: "NEW_NAME",
			});
		}
	});
});
