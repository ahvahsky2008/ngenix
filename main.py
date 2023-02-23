from multiprocessing import Pool
from uuid import uuid4
from typing import List
from zipfile import ZipFile

from random import randint, choice
from string import ascii_letters, digits
from settings import config

from lxml import etree as ET


def random_string(size=10, chars=ascii_letters + digits):
    return ''.join(choice(chars) for _ in range(size))


def create_xml() -> bytes:
    
    xml_id = str(uuid4())
    level_num = str(randint(1, 100))
    
    root = ET.Element("root")
    ET.SubElement(root, "var", name="id", value=xml_id)
    ET.SubElement(root, "var", name="level", value=level_num)
    objects = ET.SubElement(root, "objects")

    for _ in range(randint(1, 10)):
        object_name = random_string()
        ET.SubElement(objects, "object", name=object_name)

    return ET.tostring(root, pretty_print=True)


def pack_archive(name: str):
    with ZipFile(f"{config.output_dir}/{name}", "w") as zfile:
        for i in range(config.cnt_xml):
            zfile.writestr(f"{i}.xml", create_xml())


def create_archives():
    with Pool(config.cnt_parallel_threads) as pool:
        pool.map(pack_archive, [f"{i}.zip" for i in range(config.cnt_zip)])


def create_csvs():
    with open(f"{config.output_dir}/levels.csv", "w") as first, \
            open(f"{config.output_dir}/objects.csv", "w") as second:
        first.write("id,level\n")
        second.write("id,object_name\n")


def parse_xml(xml: bytes):
    doc = ET.fromstring(xml)

    vars_ = doc.findall("var")
    id_ = vars_[0].get("value")
    level = vars_[1].get("value")
    first_row = f"{id_},{level}"

    objects = doc.find("objects")
    second_rows = [f"{id_},{obj.get('name')}" for obj in objects.getchildren()]
    return first_row, second_rows


def process_all_archives():
    with open(f"{config.output_dir}/levels.csv", "a") as first, \
            open(f"{config.output_dir}/objects.csv", "a") as second:
        for i in range(config.cnt_zip):
            first_group, second_group = process_archive(f"{i}.zip")
            data_first = "\n".join(first_group) + "\n"
            first.write(data_first)
            data_second = "\n".join(second_group) + "\n"
            second.write(data_second)


def process_archive(name) -> List[list]:
    with ZipFile(f"{config.output_dir}/{name}") as zfile:
        first_group = []
        second_group = []
        for j in range(config.cnt_xml):
            f = zfile.open(f"{j}.xml")
            xml = f.read()
            first_row, second_rows = parse_xml(xml)
            first_group.append(first_row)
            second_group.extend(second_rows)
        return first_group, second_group


def main():

    create_archives()
    create_csvs()
    with Pool(config.cnt_parallel_threads) as pool:
        with open(f"{config.output_dir}/levels.csv", "a") as first, \
                open(f"{config.output_dir}/objects.csv", "a") as second:
            for result in pool.imap_unordered(process_archive, [f"{i}.zip" for i in range(config.cnt_zip)]):

                first_group, second_group = result
                data_first = "\n".join(first_group) + "\n"
                first.write(data_first)
                data_second = "\n".join(second_group) + "\n"
                second.write(data_second)


if __name__ == "__main__":
    main()