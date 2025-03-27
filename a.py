import lxml.etree as ET

count = 0
for event, elem in ET.iterparse('ingestion/HMDB_DATA/hmdb_proteins.xml', events=('end',)):
    local_tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
    if local_tag == 'protein':
        count += 1
        elem.clear()

print(count)