provider "aws" {
  region = "your-region-here"
}

# Create a Redshift cluster
resource "aws_redshift_cluster" "your-cluster" {
  cluster_identifier = "your-cluster-identifier"
  database_name      = "your-db-name"
  master_username    = "username"
  master_password    = "password"
  node_type          = "dc2.large"
  cluster_type       = "single-node"

  publicly_accessible = true

  vpc_security_group_ids = ["your-sg"]
  iam_roles              = ["your-roles"]
}

# Create an IAM user
resource "aws_iam_user" "iam-user" {
  name = "iam-name"
}

# Create an IAM policy for the user
resource "aws_iam_policy" "your-policy" {
  name        = "name-of-policy"
  description = "Policy granting access to the S3 bucket, Redshift cluster, and RDS PostgreSQL instance"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:*",
          "redshift:*",
          "rds:*"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

# Attach the policy to the IAM user
resource "aws_iam_user_policy_attachment" "your-policy-attachment" {
  user       = aws_iam_user.iam-user.name
  policy_arn = aws_iam_policy.your-policy.arn
}

# Create a security group for the Redshift cluster and RDS PostgreSQL instance
resource "aws_security_group" "your-sg" {
  name        = "your-sg-name"
  description = "Security group for Redshift cluster and RDS PostgreSQL instance"

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Create an IAM role for the Redshift cluster
resource "aws_iam_role" "your-role" {
  name = "role-name"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
}
