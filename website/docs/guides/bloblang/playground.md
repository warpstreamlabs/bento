---
title: Bloblang Playground
sidebar_label: Playground
hide_title: false
description: Interactive Bloblang editor with real-time execution and syntax highlighting
---

import PlaygroundIframe from '@site/src/components/PlaygroundIframe';

# Bloblang Playground

<div>
  <PlaygroundIframe 
    src="/bento/playground/index.html" 
    width="100%" 
    height="600px" 
    frameBorder="0"
    title="Interactive Bloblang Playground"
  />
</div>

<div style={{ marginTop: '1rem' }}>
	<a
    className="button button--primary"
    href="/bento/playground/index.html"
    target="_blank"
    rel="noopener noreferrer"
    style={{ 
      display: 'inline-flex', 
      alignItems: 'center', 
      gap: '8px', 
      padding: '8px 16px', 
      lineHeight: '1'
    }}
  >
		<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ verticalAlign: 'middle', display: 'block' }}>
			<path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/>
		</svg>
		<span style={{ display: 'inline-block', verticalAlign: 'middle' }}>Open in Full Screen</span>
	</a>
</div>

## Quick Start

Experiment with Bloblang mappings in real-time using our interactive editor.

The playground has three main panels:

1. **Input JSON** (top-left): Your source data
2. **Bloblang Mapping** (bottom): Your Bloblang logic
3. **Output** (top-right): The transformed result

## Example Transformations

### Basic Field Mapping

Try this simple transformation:

**Input:**

```json
{
  "user": {
    "firstName": "John",
    "lastName": "Doe"
  },
  "timestamp": "2023-12-01T10:00:00Z"
}
```

**Mapping:**

```coffee
root.fullName = this.user.firstName + " " + this.user.lastName
root.date = this.timestamp.format_timestamp("2006-01-02")
```

### Array Processing

Transform arrays with ease:

**Input:**

```json
{
  "products": [
    { "name": "Laptop", "price": 999.99, "category": "electronics" },
    { "name": "Book", "price": 19.99, "category": "media" }
  ]
}
```

**Mapping:**

```coffee
root.items = this.products.map_each(item -> {
  "title": item.name.uppercase(),
  "cost": item.price,
  "type": item.category
})
```

### Conditional Logic

Handle different data scenarios:

**Input:**

```json
{
  "user": { "age": 25, "country": "US" },
  "product": { "price": 50 }
}
```

**Mapping:**

```coffee
root.user = this.user
root.product = this.product

# Apply discount based on conditions
root.discount = if this.user.age < 30 && this.user.country == "US" {
  this.product.price * 0.1
} else {
  0
}

root.finalPrice = this.product.price - root.discount
```

## Tips & Tricks

### Exploring Functions

- Hover over any function in the mapping editor to see its documentation
- Use auto-completion (Ctrl+Space) to discover available functions
- Check the [function reference](/docs/guides/bloblang/functions) for comprehensive documentation

### Debugging Mappings

- Use `root.debug = this` to inspect the entire input structure
- Add temporary fields like `root.temp = this.some.field` to debug specific paths
- The error panel shows exactly where syntax errors occur

### Common Patterns

- **Flattening nested objects**: `root = this.flatten()`
- **Filtering arrays**: `root.filtered = this.items.filter(item -> item.active)`
- **Grouping data**: `root.groups = this.items.group_by(item -> item.category)`
- **String manipulation**: `root.clean = this.text.trim().lowercase()`

## Local Development

Want to run the playground locally? Use the Bento CLI:

```bash
bento blobl server
```

This starts a local server with the full playground interface, perfect for development and testing.

## Next Steps

- [Learn Bloblang syntax](/docs/guides/bloblang/about)
- [Explore function reference](/docs/guides/bloblang/functions)
- [See more examples](/docs/guides/bloblang/walkthrough)
