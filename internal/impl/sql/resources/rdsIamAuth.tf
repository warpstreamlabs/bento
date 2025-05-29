provider "aws" {
    region = "us-east-1"
}

resource "aws_vpc" "rds_vpc" {
    cidr_block = "10.0.0.0/16"
    instance_tenancy = "default"

    enable_dns_support = true
    enable_dns_hostnames = true

    tags = {
        Name = "rds-vpc"
    }
}

resource "aws_internet_gateway" "rds_vpc_gw" {
    vpc_id = aws_vpc.rds_vpc.id
    depends_on = [aws_vpc.rds_vpc]
}

resource "aws_security_group" "rds_sg" {
    vpc_id = aws_vpc.rds_vpc.id 

    ingress {
        from_port = 5432
        to_port = 5432
        protocol = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }

    egress {
        from_port = 0 
        to_port = 0 
        protocol = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }

    tags = {
        Name = "rds-sg"
    }

    depends_on = [aws_vpc.rds_vpc]
}

resource "aws_route_table" "rds_route_table" {
    vpc_id = aws_vpc.rds_vpc.id

    route {
        cidr_block = "0.0.0.0/0"
        gateway_id = aws_internet_gateway.rds_vpc_gw.id
    }

    tags = {
        Name = "rds-route-table"
    }
}

resource "aws_subnet" "rds_subnet_1" {
    vpc_id = aws_vpc.rds_vpc.id
    cidr_block = "10.0.1.0/24"
    availability_zone = "us-east-1a"
    map_public_ip_on_launch = "true"

    tags = {
        Name = "rds-subnet-1"
    }

    depends_on = [aws_vpc.rds_vpc]
}

resource "aws_subnet" "rds_subnet_2" {
    vpc_id = aws_vpc.rds_vpc.id
    cidr_block = "10.0.2.0/24"
    availability_zone = "us-east-1b"
    map_public_ip_on_launch = "true"

    tags = {
        Name = "rds-subnet-2"
    }

    depends_on = [aws_vpc.rds_vpc]
}

resource "aws_subnet" "rds_subnet_3" {
    vpc_id = aws_vpc.rds_vpc.id
    cidr_block = "10.0.3.0/24"
    availability_zone = "us-east-1c"
    map_public_ip_on_launch = "true"

    tags = {
        Name = "rds-subnet-3"
    }

    depends_on = [aws_vpc.rds_vpc]
}

resource "aws_route_table_association" "rds_subnet_1_association" {
    subnet_id = aws_subnet.rds_subnet_1.id
    route_table_id = aws_route_table.rds_route_table.id
}

resource "aws_route_table_association" "rds_subnet_2_association" {
  subnet_id      = aws_subnet.rds_subnet_2.id
  route_table_id = aws_route_table.rds_route_table.id
}

resource "aws_route_table_association" "rds_subnet_3_association" {
  subnet_id      = aws_subnet.rds_subnet_3.id
  route_table_id = aws_route_table.rds_route_table.id
}

resource "aws_db_subnet_group" "rds_subnet_group" {
  name = "rds-subnet-group"
  subnet_ids = [aws_subnet.rds_subnet_1.id, aws_subnet.rds_subnet_2.id, aws_subnet.rds_subnet_3.id]

  tags = {
    Name = "rds-subnet-group"
  }
}

resource "aws_db_instance" "rds_instance" {
  identifier           = "my-rds-instance"
  engine               = "postgres"
  engine_version       = "15"
  instance_class       = "db.t3.micro"
  allocated_storage    = 20
  storage_encrypted    = true
  db_name              = "testdb"
  username             = "masterusername"
  password             = "password123"
  skip_final_snapshot  = true
  publicly_accessible  = true
  iam_database_authentication_enabled = true
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name = aws_db_subnet_group.rds_subnet_group.name
}