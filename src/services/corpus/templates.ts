import type { NodeTypeCorpusVariant } from "./loader.js";

export function validateVariantTemplateGeneration(
  variant: NodeTypeCorpusVariant,
  options: { allowPartial?: boolean } = {}
): void {
  if (!variant.nodeDefinition) {
    throw new Error(
      `Variant ${variant.variantKey} cannot generate a workspace template because its embedded nodeMetadataSpec could not be parsed. Parse error: ${variant.parseError ?? "unknown"}`
    );
  }

  if (variant.supportStatus === "partial" && !options.allowPartial) {
    const unsupported =
      variant.unsupportedPrimitives.length > 0
        ? variant.unsupportedPrimitives.join(", ")
        : "unknown unsupported controls";
    throw new Error(
      `Variant ${variant.variantKey} is only partially supported because it uses unsupported primitives: ${unsupported}. Pass allowPartial=true to generate a best-effort template.`
    );
  }
}
