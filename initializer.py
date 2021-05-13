from collections import defaultdict
from readers import tm1_reader
from templates import url_template, mdx_SYS_LocalHierarchies, mdx_CustomerAttributes
from configuration import logger

supporting_data = {}

def download_parameters_cube(tm1, mdx, name):

    data = tm1_reader(tm1, mdx, name)

    column_axis = data['Axes'][0]
    row_axis = data['Axes'][1]
    column_members = [t['Members'][0]['Name'] for t in column_axis['Tuples']]
    row_members = [t['Members'][0]['Name'] for t in row_axis['Tuples']]
    column_cardinality = column_axis['Cardinality']
    cells = data['Cells']
    cube_as_dict = defaultdict(dict)
    for i, v in enumerate(cells):
        row_index = i // column_cardinality
        column_index = i % column_cardinality
        cube_as_dict[row_members[row_index]][column_members[column_index]] = v['Value']
    return cube_as_dict


def get_attributes(tm1, url):
    response = tm1.connection.GET(url=url)
    response_json = response.json()
    values = response_json['value']
    dimension = {}
    for v in values:
        dimension[v['Name']] = v['Attributes']
    return dimension

def load_supporting_attributes_and_cubes(tm1):

    all_data = {}

    logger.info('downloading dimensions with attributes')
    dimension_list = tm1.cubes.get_dimension_names('5050.SPOT_PL')
    dimension_list.extend(['SYS_SPOT_Parameters', 'SYS_TemplateSetUp'])

    for dimension in dimension_list:
        url = url_template.format(dimension, dimension)
        all_data[dimension] = get_attributes(tm1, url)
    logger.info('dimensions download completed')

    all_data['SYS_LocalHierarchies'] = download_parameters_cube(tm1, mdx_SYS_LocalHierarchies, 'SYS_LocalHierarchies')
    all_data['CustomerAttributes'] = download_parameters_cube(tm1, mdx_CustomerAttributes, 'CustomerAttributes')
    supporting_data.update(all_data)
