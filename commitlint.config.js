// Conventional Commits configuration for InferaDB
// See: https://www.conventionalcommits.org/
// See: https://github.com/conventional-changelog/commitlint

export default {
  extends: ["@commitlint/config-conventional"],
  rules: {
    // Types allowed in commit messages
    "type-enum": [
      2,
      "always",
      [
        "feat", // New feature (minor version bump)
        "fix", // Bug fix (patch version bump)
        "docs", // Documentation only
        "style", // Formatting, whitespace (no code change)
        "refactor", // Code change that neither fixes nor adds
        "perf", // Performance improvement (patch version bump)
        "test", // Adding or correcting tests
        "build", // Build system or external dependencies
        "ci", // CI configuration
        "chore", // Other changes that don't modify src or test
        "dx",
        "ai",
        "imp",
        "release",
      ],
    ],
    // Scope is optional but must be lowercase if provided
    "scope-case": [2, "always", "lower-case"],
    // Subject must start with lowercase
    "subject-case": [2, "always", "lower-case"],
    // No period at the end of subject
    "subject-full-stop": [2, "never", "."],
    // Subject cannot be empty
    "subject-empty": [2, "never"],
    // Type cannot be empty
    "type-empty": [2, "never"],
    // Header max length (type + scope + subject)
    "header-max-length": [2, "always", 100],
  },
};
