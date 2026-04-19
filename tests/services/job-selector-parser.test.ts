import { describe, it, expect } from "vitest";
import { parseJobSelector } from "../../src/services/jobs/selector-parser.js";

describe("parseJobSelector", () => {
  it("returns zero terms for empty input", () => {
    expect(parseJobSelector("")).toEqual({ terms: [], warnings: [] });
    expect(parseJobSelector(undefined)).toEqual({ terms: [], warnings: [] });
    expect(parseJobSelector("   ")).toEqual({ terms: [], warnings: [] });
  });

  it("parses a single subgraph term with quoted name", () => {
    const r = parseJobSelector(`{ subgraph: "DIM_DATE" }`);
    expect(r.warnings).toEqual([]);
    expect(r.terms).toEqual([{ kind: "subgraph", name: "DIM_DATE" }]);
  });

  it("parses a single location+name term without quotes", () => {
    const r = parseJobSelector(`{ location: BRONZE_STG name: STG_CASE }`);
    expect(r.warnings).toEqual([]);
    expect(r.terms).toEqual([
      { kind: "location_name", location: "BRONZE_STG", name: "STG_CASE" },
    ]);
  });

  it("preserves case of location and name", () => {
    const r = parseJobSelector(`{ location: BRONZE_SFDC_APAC name: user }`);
    expect(r.terms).toEqual([
      { kind: "location_name", location: "BRONZE_SFDC_APAC", name: "user" },
    ]);
  });

  it("splits on top-level OR (case-insensitive)", () => {
    const r = parseJobSelector(
      `{ subgraph: "A" } OR { subgraph: "B" } or { subgraph: "C" }`
    );
    expect(r.warnings).toEqual([]);
    expect(r.terms).toEqual([
      { kind: "subgraph", name: "A" },
      { kind: "subgraph", name: "B" },
      { kind: "subgraph", name: "C" },
    ]);
  });

  it("does not split on OR inside a brace body", () => {
    // Degenerate case — braces don't actually nest in real selectors, but
    // the tokenizer is depth-aware so `OR` inside a pair is treated as text.
    const r = parseJobSelector(`{ subgraph: "A_OR_B" }`);
    expect(r.terms).toEqual([{ kind: "subgraph", name: "A_OR_B" }]);
  });

  it("mixes subgraph and location+name terms", () => {
    const r = parseJobSelector(
      `{ subgraph: "SFDC_ACCOUNT" } OR { location: BRONZE_STG name: STG_CASE }`
    );
    expect(r.terms).toEqual([
      { kind: "subgraph", name: "SFDC_ACCOUNT" },
      { kind: "location_name", location: "BRONZE_STG", name: "STG_CASE" },
    ]);
    expect(r.warnings).toEqual([]);
  });

  it("warns and drops the `{ A || B }` footgun but keeps siblings", () => {
    const r = parseJobSelector(
      `{ subgraph: "A" || subgraph: "B" } OR { subgraph: "C" }`
    );
    expect(r.terms).toEqual([{ kind: "subgraph", name: "C" }]);
    expect(r.warnings).toHaveLength(1);
    expect(r.warnings[0]).toContain("|| ");
  });

  it("warns on unbraced term but keeps siblings", () => {
    const r = parseJobSelector(`subgraph: "A" OR { subgraph: "B" }`);
    expect(r.terms).toEqual([{ kind: "subgraph", name: "B" }]);
    expect(r.warnings).toHaveLength(1);
    expect(r.warnings[0]).toContain("not wrapped in braces");
  });

  it("warns on unknown clause keyword", () => {
    const r = parseJobSelector(`{ tag: foo }`);
    expect(r.terms).toEqual([]);
    expect(r.warnings).toHaveLength(1);
    expect(r.warnings[0]).toContain("unrecognized clause");
  });

  it("warns on empty subgraph name", () => {
    const r = parseJobSelector(`{ subgraph: "" }`);
    expect(r.terms).toEqual([]);
    expect(r.warnings).toHaveLength(1);
  });

  it("handles single-quoted values", () => {
    const r = parseJobSelector(`{ subgraph: 'DIM_DATE' }`);
    expect(r.terms).toEqual([{ kind: "subgraph", name: "DIM_DATE" }]);
  });

  it("parses the real-world JCI-style long OR chain", () => {
    const input =
      `{ subgraph: "SFDC_ACCOUNT" } OR { subgraph: "SFDC_CAMPAIGN" } OR ` +
      `{ location: BRONZE_STG name: STG_CASE } OR ` +
      `{ location: BRONZE_SFDC_CONSOLIDATED name: CONTACT }`;
    const r = parseJobSelector(input);
    expect(r.warnings).toEqual([]);
    expect(r.terms).toHaveLength(4);
    expect(r.terms[0]).toEqual({ kind: "subgraph", name: "SFDC_ACCOUNT" });
    expect(r.terms[3]).toEqual({
      kind: "location_name",
      location: "BRONZE_SFDC_CONSOLIDATED",
      name: "CONTACT",
    });
  });
});
