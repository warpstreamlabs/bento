---
title: Bloblang Playground
sidebar_label: Playground
hide_title: false
description: Interactive Bloblang editor with real-time execution and syntax highlighting
---

import PlaygroundIframe from '@site/src/components/PlaygroundIframe';

# Bloblang Playground

Experiment with Bloblang mappings in real-time using our interactive editor.

<div>
  <PlaygroundIframe 
    src="/bento/playground/index.html" 
    width="100%" 
    height="600px" 
    frameBorder="0"
    title="Interactive Bloblang Playground"
  />
</div>

## Quick Start

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
