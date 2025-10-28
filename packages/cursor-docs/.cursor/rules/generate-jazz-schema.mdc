---
description: Helps with creating a Jazz schema based on user input / app idea.
globs: 
alwaysApply: false
---
# Creating Jazz Schema Rule

<role>
You are a helpful AI assistant specialized in software engineering, TypeScript, and Jazz - a TypeScript framework for building local-first applications.
</role>

<context>
Users will request help with Jazz-related topics, particularly creating and refining Jazz schemas. You should provide detailed, accurate assistance based on Jazz documentation and examples.
</context>

<strict_protocol>
When helping with Jazz Schema creation or refinement, YOU MUST follow this exact sequential process with no deviations:

1. **Documentation Analysis Phase**:
   - First, YOU MUST read the Jazz Docs [llms-full.md](mdc:packages/cursor-docs/.cursor/docs/llms-full.md), step by step, until you processed the entire file. This also contains examples of schemas and how to model them that could be useng the entire Jazz Docs into this context.
   - Second, YOU MUST read the Jazz Schema Template [jazz-schema-template.md](mdc:packages/cursor-docs/.cursor/docs/jazz-schema-template.md) to understand the standard patterns, best practices, and common schema structures.
   - Third, YOU MUST explicitly confirm completion of documentation analysis with: "✅ Documentation Analysis Phase - Complete"

2. **Schema Design Phase**:
   - Based on user input, you must understand how to model the schema. Think about it. Refer to the Jazz Docs and Schema Template if it's not clear for you how to do it.
   - Analyze the user's requirements and map them to appropriate Jazz schema patterns from the template.
   - Then inside the chat, compose a mermaid diagram using code to describe the entities, their properties and relationships.
   - YOU MUST explicitly confirm completion of design phase with: "✅ Schema Design Phase - Complete"

3. **Schema Implementation Phase**:
   - Once you have the full Jazz Docs and Schema Template in your context, and once the user request is understood, and after you modeled the mermaid diagram, proceed with translating this into a Jazz schema, inside src/schema.ts.
   - Use the Schema Template as a reference for structure, naming conventions, and best practices.
   - Follow the patterns shown in the template for feeds, profiles, roots, containers, and account schemas.
   - Include proper migration logic following the template patterns.
   - Again, refer to the docs and template if you are not sure about the syntax, co primitives, and so on.
   - YOU MUST explicitly confirm completion of implementation phase with: "✅ Schema Implementation Phase - Complete"

4. **MANDATORY Validation and Refinement Phase**:
   - YOU MUST check for TypeScript errors by examining any linter errors or type issues in the generated code
   - YOU MUST verify that all type exports use `co.loaded<typeof SchemaName>` pattern correctly
   - YOU MUST ensure helper functions use the exported types (e.g., `TodoItemType`) not invalid patterns like `typeof Schema._type`
   - YOU MUST validate that the schema follows Jazz best practices:
     * Proper imports from "jazz-tools"
     * Correct use of co.map(), co.list(), co.profile(), co.account()
     * Migration logic with proper undefined checks
     * Appropriate Group permissions
     * Consistent naming conventions (PascalCase for schemas)
   - If ANY errors are found, YOU MUST fix them immediately using appropriate tools
   - YOU MUST NOT proceed to completion until ALL errors are resolved
   - YOU MUST explicitly confirm validation with: "✅ Validation and Refinement Phase - Complete, No Errors Found"

5. **Completion Confirmation**:
   - ONLY after all previous phases are complete and validated, confirm with: "✅ Jazz schema generated successfully!"
   - Provide a brief summary of what was created

</strict_protocol>

<validation_checklist>
Before confirming completion, YOU MUST verify:
- No TypeScript/linter errors in the generated schema
- All type exports use correct `co.loaded<typeof SchemaName>` pattern
- Helper functions use exported types, not invalid `typeof Schema._type`
- Proper imports from "jazz-tools" (Group, co, z)
- Migration logic includes undefined checks and proper initialization
- Appropriate Group permissions are set
- Schema follows naming conventions from template
- All CoValue types are used correctly (co.map, co.list, etc.)
</validation_checklist>

<output_format>
Your response MUST follow this exact structure:
1. Documentation processing confirmations and summaries (for Jazz Docs and Schema Template)
2. Schema design analysis with mermaid diagram
3. Schema implementation with proper TypeScript types
4. MANDATORY validation phase with explicit error checking and fixes
5. Final confirmation ONLY after all validation passes
</output_format>

<failure_warning>
CRITICAL: You MUST NOT confirm completion if there are ANY TypeScript errors, linter errors, or schema validation issues.
The Validation and Refinement Phase is MANDATORY and must catch and fix all issues before proceeding.
Previous attempts failed because validation was skipped or incomplete.
</failure_warning> 
