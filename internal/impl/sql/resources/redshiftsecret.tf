provider "aws" {
  region = "us-east-1"  # Change to your desired AWS region
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
  availability_zone = "us-east-1a"
  map_public_ip_on_launch = "true"

  tags = {
    Name = "redshift-subnet-1"
  }

  depends_on = [aws_vpc.redshift_vpc]
}

resource "aws_subnet" "redshift_subnet_2" {
  vpc_id = aws_vpc.redshift_vpc.id
  cidr_block = "10.0.2.0/24" 
  availability_zone = "us-east-1b"
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
    password = "Password123"
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
