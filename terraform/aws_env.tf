# Configure the AWS Provider

provider "aws" {
    shared_credentials_file = "/home/kyleschmidt/.aws/credentials"
    region = "us-east-1"
}

resource "aws_vpc" "insight-vpc" {
    cidr_block = "10.100.0.0/26"
}

resource "aws_subnet" "insight-subnet" {
    vpc_id = "${aws_vpc.insight-vpc.id}"
    cidr_block = "10.0.0.0/16"
    map_public_ip_on_launch = "true"
    availability_zone = "us-east-1a"

    tags {
        Name = "Insight Subnet 1A"
    }
}

resource "aws_security_group" "allow_ssh" {
    name = "allow_all"
    description = "Allow inbound SSH traffic from my IP"
    vpc_id = "${aws_vpc.insight-vpc.id}"

    ingress {
        from_port = 22
        to_port = 22
        protocol = "tcp"
        cidr_blocks = ["123.123.123.123/32"]
    }

    tags {
        Name = "Allow SSH"
    }
}

resource "aws_instance" "insight-main" {
    ami = "ami-80861296"
    instance_type = "t2.2xlarge"
    subnet_id = "${aws_subnet.insight-subnet.id}"
    vpc_security_group_ids = ["${aws_security_group.allow_ssh.id}"]
    key_name = "kyle-schmidt"
    tags {
        Name = "Insight Main"
    }
}
