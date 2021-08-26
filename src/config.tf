terraform {
  backend "s3" {
    bucket  = "cig-terraform"
    encrypt = true
    region  = "eu-west-1"
  }
}
