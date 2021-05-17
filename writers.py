from configuration import chunk_size, time_stamp, target, logger
from initializer import supporting_data



def extract_axes_from_cellset(raw_cellset_as_dict):
    raw_axes = raw_cellset_as_dict['Axes']
    axes = list()
    for axis in raw_axes:
        if axis and 'Tuples' in axis and len(axis['Tuples']) > 0:
            axes.append(axis)
    return tuple(axes)


def extract_element_names_from_members(members):
    return [m['Name'] for m in members]


def writer(data, table_name):
    if len(data['Cells']) > 0:
        csv_write(csv_buffer(shaped(record_as_dict(records(data)))), table_name)


def csv_write(data, table_name):
    with open(f'Downloads/{table_name}.csv', 'w+') as f:
        for chunk in data:
            f.writelines(chunk)


def records(data):
    # def get_parts(index):
    #     row_members = extract_element_names_from_members(row_axis['Tuples'][index]['Members'])
    #     return ','.join(row_members[:-1]), row_members, row_members[-1]

    def compose_record(row_members, currencies):
        return title_members + row_members + column_members + list(currencies.values())

    cells = data['Cells']
    column_axis, row_axis, title_axis = extract_axes_from_cellset(data)
    title_members = [t['Name'] for t in title_axis['Tuples'][0]['Members']]

    # dimensions on the column are expected to be 'Amount' e currency ('CURR_PL')  in this order
    # the name of the currency is the second element in the returned tuple
    column_members = [column_axis['Tuples'][0]['Members'][0]['Name']]
    currencies_list = [t['Members'][1]['Name'] for t in column_axis['Tuples']]
    currencies = {'USD_Reported': '0', 'USD_CC': '0', 'Local_Currency': '0', 'Consolidation_Currency': '0'}
    previous_key = [t['Name'] for t in row_axis['Tuples'][0]['Members']]
    column_cardinality = column_axis['Cardinality']

    for cell_index, cell in enumerate(cells):
        row_index = cell_index // column_cardinality
        col_index = cell_index % column_cardinality
        current_key = [t['Name'] for t in row_axis['Tuples'][row_index]['Members']]
        if current_key != previous_key:
            record = compose_record(previous_key, currencies)
            yield record
            previous_key = current_key
            currencies = {'USD_Reported': '0', 'USD_CC': '0', 'Local_Currency': '0', 'Consolidation_Currency': '0'}
        value = cell['Value'] if cell['Value'] else 0.
        currency = currencies_list[col_index]
        currencies[currency] = f'{value: #0.9f}'
    record = compose_record(previous_key, currencies)
    yield record


def record_as_dict(data):
    fields = ['VERS_Analysis', 'TIME_Months', 'MU_BusinessModel', 'CUST_Plan_Full',
              'MU_Country', 'HFM_ParentEntity', 'PDT_MosaicSKU_DoubleView_Full', 'GTM_Plan_Full',
              'LO_Plan_Full', 'SC_SPOT', 'GTM_SPOT', 'EntityType_SPOT', 'VAR_SPOT_PL', 'VAR_Values',
              'USD_Reported', 'USD_CC', 'Local_Currency', 'Consolidation_Currency']
    for record_as_list in data:
        rec_as_dict = {}
        for i, field in enumerate(record_as_list):
            rec_as_dict[fields[i]] = field
        yield rec_as_dict


def shaped(data):

    SYS_SPOT_Parameters = supporting_data['SYS_SPOT_Parameters']
    TIME_Months = supporting_data['TIME_Months']
    VERS_Analysis = supporting_data['VERS_Analysis']
    MU_BusinessModel = supporting_data['MU_BusinessModel']
    CUST_Plan_Full = supporting_data['CUST_Plan_Full']
    MU_Country = supporting_data['MU_Country']
    HFM_ParentEntity = supporting_data['HFM_ParentEntity']
    PDT_MosaicSKU_DoubleView_Full = supporting_data['PDT_MosaicSKU_DoubleView_Full']
    GTM_Plan_Full = supporting_data['GTM_Plan_Full']
    LO_Plan_Full = supporting_data['LO_Plan_Full']
    SC_SPOT = supporting_data['SC_SPOT']
    GTM_SPOT = supporting_data['GTM_SPOT']
    EntityType_SPOT = supporting_data['EntityType_SPOT']
    VAR_SPOT_PL = supporting_data['VAR_SPOT_PL']
    VAR_Values = supporting_data['VAR_Values']
    SYS_LocalHierarchies = supporting_data['SYS_LocalHierarchies']
    CustomerAttributes = supporting_data['CustomerAttributes']

    # output_record
    rec =[''] * 78
    # SYS_ID
    rec[0] = SYS_SPOT_Parameters['SPOT_SYS_ID']['Value']
    # DIM1_CDV - DIM10_CDV; DIM1_NM - DIM10_NM  sono già inizializzati a ''
    rec[53] = 'Dummy'
    rec[55] = 'Dummy'
    rec[57] = 'Dummy'
    rec[59] = 'Dummy'
    rec[61] = 'Dummy'
    rec[63] = 'Dummy'
    rec[65] = 'Dummy'
    rec[67] = 'Dummy'
    rec[69] = 'Dummy'
    rec[71] = 'Dummy'

    # TimeStamp
    rec[77] = time_stamp

    for record in data:
        # CTRY_ISO_CDV
        rec[1] = record['MU_Country'][:2]
        # MNTH
        month = record['TIME_Months']
        rec[2] = TIME_Months[month]['Reference_YTD_Month']
        # YR
        vers = record['VERS_Analysis']
        rec[3] = VERS_Analysis[vers]['Year']
        # LOCL_PROD_CMPNT_CDV
        component = record['PDT_MosaicSKU_DoubleView_Full']
        rec[4] = PDT_MosaicSKU_DoubleView_Full[component]['COMPONENT_CODE']
        # LOCL_PROD_CDV
        complex = record['PDT_MosaicSKU_DoubleView_Full']
        rec[5] = PDT_MosaicSKU_DoubleView_Full[component]['COMPLEX_CODE']
        # LOCL_CUST_CDV
        customer = record['CUST_Plan_Full']
        rec[6] = CUST_Plan_Full[customer]['CUSTOMER_CDV']
        # ACCT_ID
        rec[7] = record['VAR_SPOT_PL']
        # SCENRO_CDV
        vers = record['VERS_Analysis']
        rec[8] = VERS_Analysis[vers]['Scenario']
        # GTM_LOCL0_CDV
        rec[9] = record['GTM_Plan_Full']
        # GTM_LOCL0_NM
        rec[10] = GTM_Plan_Full[record['GTM_Plan_Full']]['Name']
        # GTM_LOCL1_CDV
        AttributeName = SYS_LocalHierarchies['LEVEL_1_CDV'].get('GTM', None)
        if AttributeName:
            rec[11] = GTM_Plan_Full[record['GTM_Plan_Full']][AttributeName]
            rec[12] = GTM_Plan_Full[rec[11]]['Name']
        # GTM_LOCL2_CDV
        AttributeName = SYS_LocalHierarchies['LEVEL_2_CDV'].get('GTM', None)
        if AttributeName:
            rec[13] = GTM_Plan_Full[record['GTM_Plan_Full']][AttributeName]
            rec[14] = GTM_Plan_Full[rec[13]]['Name']
        # GTM_LOCL3_CDV
        AttributeName = SYS_LocalHierarchies['LEVEL_3_CDV'].get('GTM', None)
        if AttributeName:
            rec[15] = GTM_Plan_Full[record['GTM_Plan_Full']][AttributeName]
            rec[16] = GTM_Plan_Full[rec[15]]['Name']
        # GTM_LOCL4_CDV
        AttributeName = SYS_LocalHierarchies['LEVEL_4_CDV'].get('GTM', None)
        if AttributeName:
            rec[17] = GTM_Plan_Full[record['GTM_Plan_Full']][AttributeName]
            rec[18] = GTM_Plan_Full[rec[17]]['Name']
        # GTM_LOCL5_CDV
        AttributeName = SYS_LocalHierarchies['LEVEL_5_CDV'].get('GTM', None)
        if AttributeName:
            rec[19] = GTM_Plan_Full[record['GTM_Plan_Full']][AttributeName]
            rec[20] = GTM_Plan_Full[rec[19]]['Name']
        # GTM_LVL1_CDV
        rec[21] = GTM_SPOT[record['GTM_SPOT']]['GTM_LVL1_CDV']
        # GTM_LVL2_CDV
        rec[22] = GTM_SPOT[record['GTM_SPOT']]['GTM_LVL2_CDV']
        # GTM_LVL3_CDV
        rec[23] = GTM_SPOT[record['GTM_SPOT']]['GTM_LVL3_CDV']
        # GTM_LVL4_CDV
        rec[24] = GTM_SPOT[record['GTM_SPOT']]['GTM_LVL4_CDV']
        # GTM_LVL5_CDV
        rec[25] = GTM_SPOT[record['GTM_SPOT']]['GTM_LVL5_CDV']
        # GTM_LVL6_CDV
        rec[26] = GTM_SPOT[record['GTM_SPOT']]['GTM_LVL6_CDV']
        # GTM_LVL7_CDV
        rec[27] = GTM_SPOT[record['GTM_SPOT']]['GTM_LVL7_CDV']
        # ########## SALES CHANNEL  ##########
        AttributeName = SYS_SPOT_Parameters['SPOT_Local_Sales_Channel (one level of the customer hierarchy)']['Value']
        rec[28] = CustomerAttributes[record['CUST_Plan_Full']][AttributeName + '_CDV']
        rec[29] = CustomerAttributes[record['CUST_Plan_Full']][AttributeName + '_NM']
        #
        # LOCL_SLS_CHNL_CDV
        AttributeName = SYS_SPOT_Parameters['SPOT_Local_Sales_Channel (one level of the customer hierarchy)']['Value']
        # LOCL_SLS_CHNL_NM
        # SLS_CHNL_LVL_1_CDV
        rec[30] = SC_SPOT[record['SC_SPOT']]['SLS_CHNL_LVL_1_CDV']
        # SLS_CHNL_LVL_2_CDV
        rec[31] = SC_SPOT[record['SC_SPOT']]['SLS_CHNL_LVL_2_CDV']
        # SLS_CHNL_LVL_3_CDV
        rec[32] = SC_SPOT[record['SC_SPOT']]['SLS_CHNL_LVL_3_CDV']
        # SLS_CHNL_LVL_4_CDV
        rec[33] = SC_SPOT[record['SC_SPOT']]['SLS_CHNL_LVL_4_CDV']
        # SLS_CHNL_LVL_5_CDV
        rec[34] = SC_SPOT[record['SC_SPOT']]['SLS_CHNL_LVL_5_CDV']
        # UNIQ_SHOPR_CHNL_CDV
        rec[35] = SC_SPOT[record['SC_SPOT']]['UNIQ_SHOPR_CHNL_CDV']
        # HFM_ENTTY_CDV
        rec[36] = record['HFM_ParentEntity']
        # HFM_ENTTY_TYP
        rec[37] = EntityType_SPOT[record['EntityType_SPOT']]['SPOT_Name']
        # BUSS_MODL
        rec[38] = record['MU_BusinessModel']
        # LOCL_CRNCY_CDV
        rec[39] = MU_Country[record['MU_Country']]['LOCAL_CURRENCY']
        # BU_CRNCY_CDV
        rec[40] = MU_Country[record['MU_Country']]['LOCAL_CURRENCY']
        # LOCL_ORG_L0_CDV
        rec[41] = record['LO_Plan_Full']
        rec[42] = LO_Plan_Full[rec[41]]['Name']
        # LOCL_ORG_L1_CDV
        AttributeName = SYS_LocalHierarchies['LEVEL_1_CDV'].get('LO', None)
        if AttributeName:
            rec[43] = LO_Plan_Full[record['LO_Plan_Full']]['AttributeName']
            rec[44] = LO_Plan_Full[rec[43]]['Name']
        # LOCL_ORG_L2_CDV
        AttributeName = SYS_LocalHierarchies['LEVEL_2_CDV'].get('LO', None)
        if AttributeName:
            rec[45] = LO_Plan_Full[record['LO_Plan_Full']]['AttributeName']
            rec[46] = LO_Plan_Full[rec[45]]['Name']
        # LOCL_ORG_L3_CDV
        AttributeName = SYS_LocalHierarchies['LEVEL_3_CDV'].get('LO', None)
        if AttributeName:
            rec[47] = LO_Plan_Full[record['LO_Plan_Full']]['AttributeName']
            rec[48] = LO_Plan_Full[rec[47]]['Name']
        # LOCL_ORG_L4_CDV
        AttributeName = SYS_LocalHierarchies['LEVEL_4_CDV'].get('LO', None)
        if AttributeName:
            rec[49] = LO_Plan_Full[record['LO_Plan_Full']]['AttributeName']
            rec[50] = LO_Plan_Full[rec[49]]['Name']
        # LOCL_ORG_L5_CDV
        AttributeName = SYS_LocalHierarchies['LEVEL_5_CDV'].get('LO', None)
        if AttributeName:
            rec[51] = LO_Plan_Full[record['LO_Plan_Full']]['AttributeName']
            rec[52] = LO_Plan_Full[rec[51]]['Name']
        # VAL_USD
        rec[73] = record['USD_Reported']
        # VAL_CC
        rec[74] = record['USD_CC']
        # VAL_LOCL_CRNCY
        rec[75] = record['Local_Currency']
        # VAL_BU_CRNCY
        rec[76] = record['Consolidation_Currency']
        yield rec


def csv_buffer(data):
    lines = []
    for line in data:
        lines.append(','.join(line) + '\n')
        if len(lines) == chunk_size:
            yield lines
            lines = []
    yield lines


