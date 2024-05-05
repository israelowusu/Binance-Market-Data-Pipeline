provider "aws" {
  region = "us-east-1"
}

# Create a Redshift cluster
resource "aws_redshift_cluster" "coinbase_cluster" {
  cluster_identifier = "coinbase-cluster"
  database_name      = "coinbasedb"
  master_username    = "coinbase"
  master_password    = "Greatness663"
  node_type          = "dc2.large"
  cluster_type       = "single-node"

  publicly_accessible = true

  vpc_security_group_ids = [aws_security_group.coinbase_sg.id]
  iam_roles              = [aws_iam_role.coinbase_role.arn]
}

# Create an IAM user
resource "aws_iam_user" "coinbase_user" {
  name = "coinbase_user"
}

# Create an IAM policy for the user
resource "aws_iam_policy" "coinbase_policy" {
  name        = "coinbase_policy"
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
resource "aws_iam_user_policy_attachment" "coinbase_attachment" {
  user       = aws_iam_user.coinbase_user.name
  policy_arn = aws_iam_policy.coinbase_policy.arn
}

# Create a security group for the Redshift cluster and RDS PostgreSQL instance
resource "aws_security_group" "coinbase_sg" {
  name        = "coinbase_sg"
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
resource "aws_iam_role" "coinbase_role" {
  name = "coinbase_role"

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