
/**
 * The Bento configuration linter uses the sentinel `# BENTO LINT DISABLE` to indicate that a config snippet
 * should be skipped from linting.
 */

const BENTO_LINT_DISABLE = '# BENTO LINT DISABLE';


/**
 * This function will strip all instances of the `BENTO_LINT_DISABLE` sentinel in all YAML code snippets & configurations.
 * 
 * TODO(gregfurman): This entire sentinel approach is kinda hacky and should be improved upon in future with an approach that doesn't 
 * require an undocumented magic comment.
 * 
 * See the lintMDSnippets function in internal/cli/lint.go for details.
 * 
*/
function process(node) {
  const isYamlCodeSnippet = node.type === 'code' && (node.lang === 'yaml' || node.lang === 'yml');
  const isLintDisabled = typeof node.value === 'string' && node.value.includes(BENTO_LINT_DISABLE);

  if (isYamlCodeSnippet && isLintDisabled) {
    node.value = node.value
      .split('\n')
      .filter(line => line.trim() !== BENTO_LINT_DISABLE)
      .join('\n')
      .replace(/^\n+/, '');
  }
  if (node.children) node.children.forEach(process);
}

// remarkStripLintDirective hooks into docusaurus' remarkPlugins attribute, allowing our scrubber to run on startup.
function remarkStripLintDirective() {
  return (tree) => process(tree);
}

module.exports = {
  remarkStripLintDirective,
}
