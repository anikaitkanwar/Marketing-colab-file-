# -*- coding: utf-8 -*-

from datetime import datetime, timedelta, date
from pytz import timezone

import psycopg2
import pandas as pd
import numpy as np

import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

from urllib.parse import unquote
import urllib.parse


# =========================
# HELPERS (NO LOGIC CHANGE)
# =========================

def execute_query(host, database, user, password, port, query):
    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cur = conn.cursor()

        cur.execute(query)
        results = cur.fetchall()
        return results

    except (Exception, psycopg2.Error) as error:
        print("Error while executing query:", error)
        return None

    finally:
        if conn:
            if cur:
                cur.close()
            conn.close()
            print("PostgreSQL connection is closed")


def generate_query(start_date, end_date, report_date):
    # ✅ QUERY LOGIC UNCHANGED
    query = f"""
    With Leads as (SELECT DISTINCT
    cat."name", mc."title", l."source", COALESCE(l."comment", l."payload"->>'ad_id') AS "comment", COUNT(DISTINCT l."userId") AS "Users",COUNT(DISTINCT l."id") AS "Leads"
FROM "Leads" l
JOIN "MasterClassSlots" mcs ON l."masterclassSlotId" = mcs."id"
JOIN "MasterClass" mc ON mc."id" = mcs."masterClassId"
JOIN "Bootcamp" b ON mc."bootcampId" = b."id"
JOIN "User" u ON b."teacherId" = u."id"
JOIN "Categories" cat ON u."categoryId" = cat."id"
WHERE l."createdAt" BETWEEN '{start_date}' AND '{end_date}'
    AND l."source" IN ('facebook', 'google', 'Taboola','zepto','swiggy','gpay','zomato','cred','paytm','twitter')
GROUP BY l."source", COALESCE(l."comment", l."payload"->>'ad_id'), mc."title", cat."name"),

Joins as (SELECT DISTINCT
    cat."name", mc."title", l."source", COALESCE(l."comment", l."payload"->>'ad_id') AS "comment", COUNT(DISTINCT l."userId") AS "Joins",Count(distinct(concat(l."userId",mca."meetingId"))) as total_joins
FROM "Leads" l
JOIN "MasterClassSlots" mcs ON l."masterclassSlotId" = mcs."id"
JOIN "MasterClass" mc ON mc."id" = mcs."masterClassId"
JOIN "Bootcamp" b ON mc."bootcampId" = b."id"
JOIN "User" u ON b."teacherId" = u."id"
JOIN "Categories" cat ON u."categoryId" = cat."id"
Join "MasterclassAttendees" mca on mca."userId" = l."userId"
WHERE l."createdAt" BETWEEN '{start_date}' AND '{end_date}'
        and mca."createdAt" BETWEEN l."createdAt" AND '{end_date}'
    AND l."source" IN ('facebook', 'google', 'Taboola','zepto','swiggy','gpay','zomato','cred','paytm','twitter')
GROUP BY l."source", COALESCE(l."comment", l."payload"->>'ad_id'), mc."title", cat."name"),

rev as (with payss as (SELECT DISTINCT
    cat."name", mc."title", l."source", COALESCE(l."comment", l."payload"->>'ad_id') AS "comment", pi."id", pi."amount"
FROM "Leads" l
JOIN "MasterClassSlots" mcs ON l."masterclassSlotId" = mcs."id"
JOIN "MasterClass" mc ON mc."id" = mcs."masterClassId"
JOIN "Bootcamp" b ON mc."bootcampId" = b."id"
JOIN "User" u ON b."teacherId" = u."id"
JOIN "Categories" cat ON u."categoryId" = cat."id"
Join "PaymentIntent" pi on pi."userId" = l."userId"
WHERE l."createdAt" BETWEEN '{start_date}' AND '{end_date}'
        and pi."createdAt" BETWEEN l."createdAt" - interval '2 mins' AND '{end_date}'
        and pi."status" = '1'
    AND l."source" IN ('facebook', 'google', 'Taboola','zepto','swiggy','gpay','zomato','cred','paytm','twitter'))

select "name", "title", "source", "comment",  sum(cast ("amount" as numeric)/100) as "Total_Revenue"
from "payss"
group by "name", "title", "source", "comment"),


sales as ( SELECT DISTINCT
    cat."name", mc."title", l."source", COALESCE(l."comment", l."payload"->>'ad_id') AS "comment", count(distinct pi."id" ) as "Sales_Count"
FROM "Leads" l
JOIN "MasterClassSlots" mcs ON l."masterclassSlotId" = mcs."id"
JOIN "MasterClass" mc ON mc."id" = mcs."masterClassId"
JOIN "Bootcamp" b ON mc."bootcampId" = b."id"
JOIN "User" u ON b."teacherId" = u."id"
JOIN "Categories" cat ON u."categoryId" = cat."id"
Join "PaymentIntent" pi on pi."userId" = l."userId"
WHERE l."createdAt" BETWEEN '{start_date}' AND '{end_date}'
        and pi."createdAt" BETWEEN l."createdAt" AND '{end_date}'
        and round (cast (pi."amount" as numeric)) > 29900
        and pi."status" = '1'
    AND l."source" IN ('facebook', 'google','Taboola','zepto','swiggy','gpay','zomato','cred','paytm','twitter')
GROUP BY l."source", COALESCE(l."comment", l."payload"->>'ad_id'), mc."title", cat."name")


SELECT
    Leads."name", Leads."title", Leads."source",  Leads."comment", Leads."Users",Leads."Leads", Joins."Joins",Joins."total_joins", rev."Total_Revenue", sales."Sales_Count"
FROM
    Leads
LEFT JOIN
    Joins ON Leads."name" = Joins."name"
    AND Leads."title" = Joins."title"
    AND Leads."source" = Joins."source"
    AND Leads."comment" = Joins."comment"
LEFT JOIN
    rev ON Leads."name" = rev."name"
    AND Leads."title" = rev."title"
    AND Leads."source" = rev."source"
    AND Leads."comment" = rev."comment"
LEFT JOIN
    sales ON Leads."name" = sales."name"
    AND Leads."title" = sales."title"
    AND Leads."source" = sales."source"
    AND Leads."comment" = sales."comment";
    """
    return query


def overall_spend(report_date, starting_report_date):
    query2 = f'''
     SELECT
    a.platform,
    a."campaignName",
    a."adName",
    a."adsetName",
    a."landingUrl",
    "adId",
    a.spend AS spends,
    a.impressions AS impressions,

        CASE
            WHEN a.platform = 'facebook' THEN a."inlineLinkClicks"
            ELSE a.clicks
        END
     AS clicks
FROM "AdStats" a
WHERE a."reportDate" BETWEEN '{starting_report_date}' AND '{report_date}'
GROUP BY
    a.platform,
    a."campaignName",
    a."adName",
    a."adsetName",
    a."landingUrl",
    a."adId",
    a.spend,
    a.impressions,
    a."inlineLinkClicks",
    a.clicks
        '''
    return query2


def unquoted_url(url):
    return unquote(url)


def extract_comment_raw(url):
    try:
        qs = urllib.parse.urlparse(url).query
        for part in qs.split('&'):
            if part.startswith('comment='):
                return part[len('comment='):]
        return None
    except:
        return None


def upload_chunked_dataframe(df, worksheet, chunk_size=1000):
    for start_row in range(0, len(df), chunk_size):
        chunk = df.iloc[start_row:start_row + chunk_size]
        set_with_dataframe(worksheet, chunk, row=start_row + 1, include_column_header=(start_row == 0))


# =========================
# MAIN FUNCTION FOR STREAMLIT
# =========================
def run_pipeline(
    report_date_input: str,
    delay: int,
    user_name: str = None,
    user: str = None,
    spreadsheet_url: str = "",
    category: str = "",
    worksheet_name: str = "",
    host: str = "",
    database: str = "",
    db_user: str = "",
    password: str = "",
    port: str = "",
    json_key: dict = None
):
    if user_name is None:
        user_name = user

    """
    ✅ This is Streamlit callable.
    ✅ All your original logic is executed INSIDE this function.
    ✅ Query logic unchanged.
    """

    ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f')
    print('last used by', user_name, 'at', ind_time)

    report_date = datetime.strptime(report_date_input, "%Y-%m-%d")
    start_date = (report_date - timedelta(days=delay)).strftime("%Y-%m-%d 18:30:00")
    end_date = report_date.strftime("%Y-%m-%d 18:30:00")

    # ---- Revenue Funnel Query ----
    query = generate_query(start_date, end_date, report_date.strftime("%Y-%m-%d"))
    results = execute_query(host, database, db_user, password, port, query)

    if results is None:
        raise Exception("DB query returned no results")

    df = pd.DataFrame(results)
    df.columns = ['Category','Title','Source','Comment','Users',"Leads",'Joins','Total Joins','Revenue','Converted']

    df['Revenue'] = pd.to_numeric(df['Revenue'], errors='coerce')
    df['Converted'] = pd.to_numeric(df['Converted'], errors='coerce')
    df['Leads'] = pd.to_numeric(df['Leads'], errors='coerce')

    df['Joins'] = df['Joins'].fillna(0).astype(int)
    df['Users'] = df['Users'].astype(int)

    df_grouped = df.groupby(['Comment', 'Source', 'Category','Title'], as_index=False).agg(
        Users=('Users', 'sum'),
        Leads=('Leads', 'sum'),
        Joins=('Joins','sum'),
        Revenue=('Revenue', 'sum'),
        Converted=('Converted', 'sum')
    )

    # ---- Spend Query ----
    start_report_date = (report_date - timedelta(days=delay-1))
    query2 = overall_spend(report_date, start_report_date)
    google_spending = execute_query(host, database, db_user, password, port, query2)

    spending_df = pd.DataFrame(google_spending)
    spending_df.columns = ['Platform', 'campaignName', 'adName', 'adsetName', 'landingUrl', 'adId','spend', 'impressions', 'clicks']

    cols = ['spend', 'impressions', 'clicks']
    spending_df[cols] = spending_df[cols].where(spending_df[cols] >= 0, 1)

    adId_df = spending_df.groupby('adId').agg(
        {'spend':'sum','impressions':'sum','clicks': 'sum'}
    ).reset_index()

    rev_funnel = df_grouped.copy()
    spend_funnel = spending_df.copy()

    rev_funnel['Comment'] = rev_funnel['Comment'].str.replace(' ', '+', regex=False)
    rev_funnel['Comment'] = rev_funnel['Comment'].str.replace('$', '', regex=False)

    spend_funnel['landingUrl'] = spend_funnel['landingUrl'].apply(unquoted_url)
    spend_funnel = spend_funnel.replace(-1, 0)
    spend_funnel['campaignName'] = spend_funnel['campaignName'].str.replace('$', '', regex=False)
    spend_funnel['adName'] = spend_funnel['adName'].str.replace('$', '', regex=False)
    spend_funnel['adsetName'] = spend_funnel['adsetName'].str.replace('$', '', regex=False)
    spend_funnel['landingUrl'] = spend_funnel['landingUrl'].str.replace('$', '', regex=False)

    spend_funnel['landing_para'] = spend_funnel['landingUrl'].apply(extract_comment_raw)
    spend_funnel['landing_para'] = spend_funnel['landing_para'].astype(str).str.replace('$', '', regex=False)
    spend_funnel = spend_funnel.fillna(0)

    landing_para_df = spend_funnel.groupby(['landing_para','campaignName','adName','adsetName']).agg(
        {'spend':'sum','impressions':'sum','clicks': 'sum'}
    ).reset_index()

    rev_funnel['Comment'] = rev_funnel['Comment'].str.split('@@').str[0]
    rev_funnel = rev_funnel.groupby(['Category','Title','Source','Comment'], as_index=False).sum(numeric_only=True)

    grouped_spend = spend_funnel.groupby(['adName', 'Platform', 'adsetName','campaignName'], as_index=False).sum(['spend','impressions','clicks'])
    grouped_spend_df = grouped_spend.copy()

    grouped_spend_df['key2'] = grouped_spend_df['campaignName'].astype(str) + grouped_spend_df['adsetName'].astype(str) + grouped_spend_df['adName'].astype(str)

    matched = pd.merge(rev_funnel, grouped_spend_df, left_on='Comment', right_on='key2', how='left')
    s1 = matched[~(matched['spend'].isna())]
    unmatched = matched[(matched['spend'].isna())]
    unmatched.dropna(axis=1, how='all', inplace=True)

    matched = pd.merge(unmatched, adId_df, left_on='Comment', right_on='adId', how='left')
    s2 = matched[~(matched['spend'].isna())]
    s2['campaignName'] = 0
    s2['adsetName'] = 0
    s2['adName'] = 0

    unmatched = matched[(matched['spend'].isna())]
    unmatched.dropna(axis=1, how='all', inplace=True)

    matched = pd.merge(unmatched, spending_df, left_on='Comment', right_on='adName', how='left')
    s3 = matched[~(matched['spend'].isna())]

    unmatched = matched[(matched['spend'].isna())]
    unmatched.dropna(axis=1, how='all', inplace=True)

    matched = pd.merge(unmatched, landing_para_df, left_on='Comment', right_on='landing_para', how='left')
    s4 = matched[~(matched['spend'].isna())]

    unmatched = matched[(matched['spend'].isna())]
    unmatched.dropna(axis=1, how='all', inplace=True)

    grouped_spend_df['key1'] = grouped_spend_df['campaignName'].astype(str) + grouped_spend_df['adName'].astype(str)
    matched = pd.merge(unmatched, grouped_spend_df, left_on='Comment', right_on='key1', how='left')
    s5 = matched[~(matched['spend'].isna())]

    unmatched = matched[(matched['spend'].isna())]

    columns = ['Category','Title','Source','Comment','campaignName','adsetName','adName','spend','impressions','clicks','Users','Leads','Joins','Converted','Revenue']
    s1 = s1[columns]
    s2 = s2[columns]
    s3 = s3[columns]
    s4 = s4[columns]
    s5 = s5[columns]

    matched_all = pd.concat([s1, s2, s3, s4, s5], axis=0)

    try:
        unmatched = unmatched.drop(['key1', 'key2'], axis=1)
    except:
        pass

    final_result = pd.concat([matched_all, unmatched], axis=0)

    final_result['CPL'] = final_result['spend']/final_result['Users']
    final_result['ROI'] =((final_result['Revenue']/final_result['spend'])-1)*100
    final_result['Joining perc'] = (final_result['Joins']/final_result['Users'])*100
    final_result['Conversion percent'] = (final_result['Converted']/final_result['Joins'])*100

    mapped_camp = final_result[['Category','Source','Title','Comment','campaignName','adsetName','adName','spend','impressions','clicks','Users','Leads','CPL','Joins','Joining perc','Converted','Conversion percent','Revenue','ROI']]

    search_campaigns = mapped_camp[
        (mapped_camp['Comment'].str.contains('twfu|tfal',case=False, na=False)) &
        (mapped_camp['Comment'].str.contains('search',case=False, na=False))
    ]

    grouped = search_campaigns.groupby('Comment').agg({
        'Category':'first',
        'Source':'first',
        'campaignName':'first',
        'adsetName':'first',
        'adName':'first',
        'spend': 'first',
        'impressions': 'first',
        'clicks': 'first',
        'Users': 'sum',
        'Leads': 'sum',
        'Joins': 'sum',
        'Revenue': 'sum',
        'Converted': 'sum',
        'Title': lambda x: ', '.join(x.unique())
    }).reset_index()

    mapped_camp = mapped_camp[
        ~((mapped_camp['Comment'].str.contains('twfu|tfal', case=False, na=False)) &
          (mapped_camp['Comment'].str.contains('search', case=False, na=False)))
    ]

    Corrected_spends = pd.concat([mapped_camp, grouped])
    Corrected_spends = Corrected_spends[Corrected_spends['Category'] == category]

    # ---- Google Sheet Upload ----
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(json_key, scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_url(spreadsheet_url)
    sheet_name = f"{worksheet_name}"

    stacked_df_categorized = Corrected_spends.replace([np.inf, -np.inf], np.nan).fillna(0)

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=20)

    upload_chunked_dataframe(stacked_df_categorized, worksheet)

    worksheet.format("A:M", {
        "horizontalAlignment": "CENTER",
        "textFormat": {"fontSize": 11}
    })

    return stacked_df_categorized
