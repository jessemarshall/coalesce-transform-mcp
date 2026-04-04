import { homedir } from "node:os";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  RESOURCE_FILES,
  OVERRIDE_MARKER,
  extractResourceName,
  initializeSkillsDir,
} from "../resources/index.js";

const DEFAULT_SKILLS_DIR = join(homedir(), ".coalesce-mcp", "skills");

const LOCAL_WRITE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: true,
  destructiveHint: false,
  openWorldHint: false,
} as const;

export interface PersonalizeSkillsResult {
  directory: string;
  created: string[];
  alreadyExisted: string[];
  totalSkills: number;
  configHint: string | null;
}

function personalizeSkills(directory?: string): PersonalizeSkillsResult {
  const skillsDir = directory || DEFAULT_SKILLS_DIR;

  // Track which files exist before seeding
  const created: string[] = [];
  const alreadyExisted: string[] = [];

  for (const relativePath of Object.values(RESOURCE_FILES)) {
    const resourceName = extractResourceName(relativePath);
    const skillsFile = join(skillsDir, `coalesce_skills.${resourceName}.md`);
    const userFile = join(skillsDir, `user_skills.${resourceName}.md`);

    const skillsExisted = existsSync(skillsFile);
    const userExisted = existsSync(userFile);

    if (skillsExisted && userExisted) {
      alreadyExisted.push(resourceName);
    } else {
      created.push(resourceName);
    }
  }

  // Do the actual seeding
  initializeSkillsDir(skillsDir);

  // Verify seeding succeeded — initializeSkillsDir swallows errors internally
  if (!existsSync(skillsDir)) {
    throw new Error(`Directory was not created: ${skillsDir}`);
  }

  const envIsSet = process.env.COALESCE_MCP_SKILLS_DIR === skillsDir;

  return {
    directory: skillsDir,
    created,
    alreadyExisted,
    totalSkills: Object.keys(RESOURCE_FILES).length,
    configHint: envIsSet
      ? null
      : `Set COALESCE_MCP_SKILLS_DIR="${skillsDir}" in your MCP server config to activate these customizations.`,
  };
}

export function registerSkillTools(server: McpServer, _client: CoalesceClient): void {
  server.registerTool(
    "personalize_skills",
    {
      title: "Personalize Skills",
      description:
        "Export all bundled Coalesce skill files to a local directory for customization. " +
        "Each skill gets two files: coalesce_skills.{name}.md (the default) and user_skills.{name}.md (your overrides). " +
        "Edit user_skills files to augment or override the defaults. Idempotent — never overwrites existing files.",
      inputSchema: z.object({
        directory: z
          .string()
          .optional()
          .describe(
            `Directory to export skills to. Defaults to ${DEFAULT_SKILLS_DIR}`
          ),
      }),
      annotations: LOCAL_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = personalizeSkills(params.directory);

        const lines: string[] = [
          `Personalized ${result.totalSkills} skills to: ${result.directory}`,
          "",
        ];

        if (result.created.length > 0) {
          lines.push(`Created ${result.created.length} new skill(s):`);
          for (const name of result.created) {
            lines.push(`  - ${name}`);
          }
        }

        if (result.alreadyExisted.length > 0) {
          lines.push(
            `${result.alreadyExisted.length} skill(s) already existed (unchanged).`
          );
        }

        lines.push("");
        lines.push("How to customize:");
        lines.push(
          `  - Edit user_skills.{name}.md to add your own guidance (augments the default)`
        );
        lines.push(
          `  - Add "${OVERRIDE_MARKER}" as the first line to replace the default entirely`
        );
        lines.push(
          `  - Delete both files for a skill to disable it`
        );

        if (result.configHint) {
          lines.push("");
          lines.push(result.configHint);
        }

        return {
          content: [{ type: "text" as const, text: lines.join("\n") }],
          structuredContent: result as unknown as Record<string, unknown>,
        };
      } catch (error) {
        const message =
          error instanceof Error ? error.message : String(error);
        return {
          isError: true,
          content: [
            {
              type: "text" as const,
              text: `Failed to personalize skills: ${message}`,
            },
          ],
        };
      }
    }
  );
}
