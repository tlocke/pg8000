import re


def parse_server_version(version):
    major = re.match(r"\d+", version).group()  # leading digits in 17.0, 17rc1
    return int(major)
