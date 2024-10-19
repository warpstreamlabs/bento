---
slug: redshift
title: redshift
description: Hook up Bento with a Redshift Cluster
---

```terraform 
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}
```