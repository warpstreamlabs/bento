---
slug: redshift
title: Redshift
description: Hook up Bento with a Redshift Cluster
---

This cookbook demonstrates how to connect Bento to an Amazon Redshift cluster. 

There is an example a terraform configuration at the bottom of this page.

_______________________

So you are going to need: 
  - A Redshift Cluster
  - A Secret in SecretManager, with username + password keys as the secret value
  - Some AWS Access Keys Bento can use to get the Secret


```
redshift_endpoint = "my-redshift-cluster.[...].eu-west-1.redshift.amazonaws.com:5439"
redshift_master_password_secret_name = "redshift-password"
```

_______________________

## Bento Config

You will need to create an AWS Access Key that can retrieve the aforementioned secret, in the below example we have used `credentials` in the config, but you can configure this a number of ways, to find out what options you have you can [read the guide][credentials].

```yaml
output:
  sql_insert:
    driver: postgres 
    dsn: postgresql://username_from_secret:password_from_secret@"$REDSHIFT_ENDPOINT"/dev
    table: test
    columns: [age, name]
    args_mapping: |
      root = [
        this.age,
        this.name,
      ]
    init_statement: |
      CREATE TABLE test (name varchar(255), age int);
    secret_name: "$REDSHIFT_SECRET_NAME"
    region: eu-west-1
    credentials:
      id: "$AWS_ACCESS_KEY_ID"
      secret: "$AWS_SECRET_ACCESS_KEY"
```

 - `driver`: Redshift is compatible with PostgreSQL, so we set the driver field to `postgres`
 - `dsn`: We configure this to connect to Redshift, but the username + password we have them set to `username_from_secret` & `password_from_secret`, Bento will fetch the secret and update the DSN to use them. Bento will expect that the secret will have the keys 'username' & 'password'. Below is a standalone terraform module that will create a redshift cluster and store the username + password in a secret. Note: The username + password will be overwritten after we have the value from Secret Manager, but we still need to provide a DSN with a valid "shape" so it can be parsed correctly. 
 - `secret_name`: Here we give the secret_name of the secret Bento will use to update the DSN. 
 - `credentials`: This is one way to give the Bento Config access to the Secret in AWS. There are other ways explained [here][credentials].

The other fields are can be looked up on the [sql_insert][credentials] page. The above config makes use of [environment variable interpolation][env_var_interpolation]. 

The secret should be stored in the AWS secret manager with the name you specified in the configuration above. The value of the secret should be a JSON documentation with a username/password:

```json
{
  "username": "some_username",
  "password": "some_password"
}
```

Below is a sample standalone terraform module that creates a redshift cluster, username/password, and stores the username/password in AWS Secret manager in the right format for Bento to read it.

_______________________
## terraform
```terraform 
provider "aws" {
  region = "eu-west-1"  # Change to your desired AWS region
}

resource "random_password" "password" {
  length           = 16
  special          = true
  override_special = "!$%&*()-_=+[]{}<>:?"
}

resource "aws_vpc" "redshift_vpc" {
  cidr_block = "10.0.0.0/16"
  instance_tenancy = "default"
  tags = {
    Name = "redshift-vpc"
  }
}

resource "aws_internet_gateway" "redshift_vpc_gw" {
  vpc_id = aws_vpc.redshift_vpc.id
  depends_on = [aws_vpc.redshift_vpc]
}

resource "aws_security_group" "redshift_sg" {
  vpc_id = aws_vpc.redshift_vpc.id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Change to your specific IP range
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "redshift-sg"
  }

  depends_on = [aws_vpc.redshift_vpc]
}

resource "aws_route_table" "redshift_route_table" {
  vpc_id = aws_vpc.redshift_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.redshift_vpc_gw.id
  }

  tags = {
    Name = "redshift-route-table"
  }
}

resource "aws_subnet" "redshift_subnet_1" {
  vpc_id = aws_vpc.redshift_vpc.id
  cidr_block = "10.0.1.0/24"
  availability_zone = "eu-west-1a"
  map_public_ip_on_launch = "true"

  tags = {
    Name = "redshift-subnet-1"
  }

  depends_on = [aws_vpc.redshift_vpc]
}

resource "aws_subnet" "redshift_subnet_2" {
  vpc_id = aws_vpc.redshift_vpc.id
  cidr_block = "10.0.2.0/24" 
  availability_zone = "eu-west-1b"
  map_public_ip_on_launch = "true"

  tags = {
    Name = "redshift-subnet-2"
  }

  depends_on = [aws_vpc.redshift_vpc]
}

resource "aws_route_table_association" "redshift_subnet_1_association" {
  subnet_id      = aws_subnet.redshift_subnet_1.id
  route_table_id = aws_route_table.redshift_route_table.id
}

resource "aws_route_table_association" "redshift_subnet_2_association" {
  subnet_id      = aws_subnet.redshift_subnet_2.id
  route_table_id = aws_route_table.redshift_route_table.id
}

resource "aws_redshift_subnet_group" "redshift_subnet_group" {
  name = "redshift-subnet-group"
  subnet_ids = [aws_subnet.redshift_subnet_1.id, aws_subnet.redshift_subnet_2.id]

  tags = {
    Name = "redshift-subnet-group"
  }

}

resource "aws_secretsmanager_secret" "redshift_master_password" {
  name        = "redshift-password"
}

resource "aws_secretsmanager_secret_version" "redshift_master_password_version" {
  secret_id     = aws_secretsmanager_secret.redshift_master_password.id
  secret_string = jsonencode({
    username = "admin"
    password = random_password.password.result
  })
}

resource "aws_redshift_cluster" "my_redshift" {
  cluster_identifier      = "my-redshift-cluster"
  database_name           = "dev"

  # Use the Secrets Manager secret to get the username and password
  master_username         = jsondecode(aws_secretsmanager_secret_version.redshift_master_password_version.secret_string)["username"]
  master_password         = jsondecode(aws_secretsmanager_secret_version.redshift_master_password_version.secret_string)["password"]

  node_type               = "dc2.large"
  cluster_type            = "single-node"
  number_of_nodes         = 1
  cluster_subnet_group_name = aws_redshift_subnet_group.redshift_subnet_group.id
  port                    = 5439
  publicly_accessible     = true
  vpc_security_group_ids  = [aws_security_group.redshift_sg.id]
  skip_final_snapshot     = true

  tags = {
    Name = "MyRedshiftCluster"
  }

  depends_on = [
    aws_vpc.redshift_vpc,
    aws_security_group.redshift_sg,
    aws_redshift_subnet_group.redshift_subnet_group,
  ]
}

output "redshift_endpoint" {
  value = aws_redshift_cluster.my_redshift.endpoint
}

output "redshift_master_password_secret_name" {
  value = aws_secretsmanager_secret.redshift_master_password.name
}
```

[terraform]: /docs/cookbooks/redshift#terraform
[credentials]: /docs/guides/cloud/aws
[sql_insert]: /docs/components/outputs/sql_insert
[env_var_interpolation]: /docs/configuration/interpolation