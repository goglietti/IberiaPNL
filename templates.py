mdx_template = """
    SELECT NON EMPTY 
        {{TM1FILTERBYLEVEL({{TM1SUBSETALL([MU_Country])}},0)}} * 
        {{TM1FILTERBYLEVEL({{TM1SUBSETALL([HFM_ParentEntity])}},0)}} * 
        {{TM1FILTERBYLEVEL({{TM1SUBSETALL([PDT_MosaicSKU_DoubleView_Full])}},0)}} * 
        {{TM1FILTERBYLEVEL({{TM1SUBSETALL([GTM_Plan_Full])}},0)}} * 
        {{TM1FILTERBYLEVEL({{TM1SUBSETALL([LO_Plan_Full])}},0)}} * 
        {{TM1FILTERBYLEVEL({{TM1SUBSETALL([SC_SPOT])}},0)}} * 
        {{TM1FILTERBYLEVEL({{TM1SUBSETALL([GTM_SPOT])}},0)}} *
        {{TM1FILTERBYLEVEL({{TM1SUBSETALL([EntityType_SPOT])}},0)}} *
        {{TM1FILTERBYLEVEL({{TM1SUBSETALL([VAR_SPOT_PL])}},0), [VAR_SPOT_PL].[101]}}
     ON ROWS, 
        NON EMPTY {{[VAR_Values].[Amount]}} *
        {{[CURR_PL].[USD_Reported],[CURR_PL].[USD_CC],[CURR_PL].[Local_Currency],[CURR_PL].[Consolidation_Currency]}}
    ON COLUMNS 
    FROM [5050.SPOT_PL] 
    WHERE (
        [VERS_Analysis].[Actual_2021],
        [TIME_Months].[YTD_Feb],
        [MU_BusinessModel].[COBO],
        [CUST_Plan_Full].[{}]
 )
"""

url_template = "/api/v1/Dimensions('{}')/Hierarchies('{}')/Elements?$select=Name,Attributes"

mdx_SYS_LocalHierarchies = """
    SELECT NON EMPTY 
        {{TM1SUBSETALL([SYS_Local_Hierarchies_Levels])}}
     ON ROWS, 
        NON EMPTY {{TM1SUBSETALL([SYS_Dimensions])}}
    on COLUMNS 
    FROM [SYS_LocalHierarchies] 
 )
"""

mdx_CustomerAttributes = """
    SELECT NON EMPTY 
        {{TM1SUBSETALL([CUST_Plan_MasterDataLoad])}}
     ON ROWS, 
        NON EMPTY {{TM1SUBSETALL([CUST_AttributeList])}}
    on COLUMNS 
    FROM [0110.CustomerAttributes] 
 )
"""

create_stmt = """
DROP TABLE IF EXISTS [dbo].[Cells];
CREATE TABLE [dbo].[Cells](
[SYS_ID] varchar(255) NULL,
[CTRY_ISO_CDV] varchar(255) NULL,
[MNTH] varchar(255) NULL,
[YR] varchar(255) NULL,
[LOCL_PROD_CMPNT_CDV] varchar(255) NULL,
[LOCL_PROD_CDV] varchar(255) NULL,
[LOCL_CUST_CDV] varchar(255) NULL,
[ACCT_ID] varchar(255) NULL,
[SCENRO_CDV] varchar(255) NULL,
[GTM_LOCL0_CDV] varchar(255) NULL,
[GTM_LOCL0_NM] varchar(255) NULL,
[GTM_LOCL1_CDV] varchar(255) NULL,
[GTM_LOCL1_NM] varchar(255) NULL,
[GTM_LOCL2_CDV] varchar(255) NULL,
[GTM_LOCL2_NM] varchar(255) NULL,
[GTM_LOCL3_CDV] varchar(255) NULL,
[GTM_LOCL3_NM] varchar(255) NULL,
[GTM_LOCL4_CDV] varchar(255) NULL,
[GTM_LOCL4_NM] varchar(255) NULL,
[GTM_LOCL5_CDV] varchar(255) NULL,
[GTM_LOCL5_NM] varchar(255) NULL,
[GTM_LVL1_CDV] varchar(255) NULL,
[GTM_LVL2_CDV] varchar(255) NULL,
[GTM_LVL3_CDV] varchar(255) NULL,
[GTM_LVL4_CDV] varchar(255) NULL,
[GTM_LVL5_CDV] varchar(255) NULL,
[GTM_LVL6_CDV] varchar(255) NULL,
[GTM_LVL7_CDV] varchar(255) NULL,
[LOCL_SLS_CHNL_CDV] varchar(255) NULL,
[LOCL_SLS_CHNL_NM] varchar(255) NULL,
[SLS_CHNL_LVL_1_CDV] varchar(255) NULL,
[SLS_CHNL_LVL_2_CDV] varchar(255) NULL,
[SLS_CHNL_LVL_3_CDV] varchar(255) NULL,
[SLS_CHNL_LVL_4_CDV] varchar(255) NULL,
[SLS_CHNL_LVL_5_CDV] varchar(255) NULL,
[UNIQ_SHOPR_CHNL_CDV] varchar(255) NULL,
[HFM_ENTTY_CDV] varchar(255) NULL,
[HFM_ENTTY_TYP] varchar(255) NULL,
[BUSS_MODL] varchar(255) NULL,
[LOCL_CRNCY_CDV] varchar(255) NULL,
[BU_CRNCY_CDV] varchar(255) NULL,
[LOCL_ORG_L0_CDV] varchar(255) NULL,
[LOCL_ORG_L0_NM] varchar(255) NULL,
[LOCL_ORG_L1_CDV] varchar(255) NULL,
[LOCL_ORG_L1_NM] varchar(255) NULL,
[LOCL_ORG_L2_CDV] varchar(255) NULL,
[LOCL_ORG_L2_NM] varchar(255) NULL,
[LOCL_ORG_L3_CDV] varchar(255) NULL,
[LOCL_ORG_L3_NM] varchar(255) NULL,
[LOCL_ORG_L4_CDV] varchar(255) NULL,
[LOCL_ORG_L4_NM] varchar(255) NULL,
[LOCL_ORG_L5_CDV] varchar(255) NULL,
[LOCL_ORG_L5_NM] varchar(255) NULL,
[DIM1_CDV] varchar(255) NULL,
[DIM1_NM] varchar(255) NULL,
[DIM2_CDV] varchar(255) NULL,
[DIM2_NM] varchar(255) NULL,
[DIM3_CDV] varchar(255) NULL,
[DIM3_NM] varchar(255) NULL,
[DIM4_CDV] varchar(255) NULL,
[DIM4_NM] varchar(255) NULL,
[DIM5_CDV] varchar(255) NULL,
[DIM5_NM] varchar(255) NULL,
[DIM6_CDV] varchar(255) NULL,
[DIM6_NM] varchar(255) NULL,
[DIM7_CDV] varchar(255) NULL,
[DIM7_NM] varchar(255) NULL,
[DIM8_CDV] varchar(255) NULL,
[DIM8_NM] varchar(255) NULL,
[DIM9_CDV] varchar(255) NULL,
[DIM9_NM] varchar(255) NULL,
[DIM10_CDV] varchar(255) NULL,
[DIM10_NM] varchar(255) NULL,
[VAL_USD] Numeric(18,4) NULL,
[VAL_CC] Numeric(18,4) NULL,
[VAL_LOCL_CRNCY] Numeric(18,4) NULL,
[VAL_BU_CRNCY] Numeric(18,4) NULL,
[SRC_EXTR_DTSTMP] varchar(255) NULL
) ON [PRIMARY]
"""
