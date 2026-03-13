#
#
#     start_as_sql_date = start.strftime("%Y-%m-%dT00:00:00Z")
#     end_plus_one_as_sql_date = (end + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
#
#     # Initialize the CloudTrail client
#     client = boto3.client('cloudtrail')
#
#     # Define your SQL query and Event Data Store ARN
#     sql_query = f"""
# SELECT
#  eventName, eventCategory, userAgent, count(*) as event_count
# FROM f8ea5e14-30db-4bf6-8c72-b58845b5315a
#  WHERE eventTime >= TIMESTAMP '{start_as_sql_date}'
#     AND eventTime < TIMESTAMP '{end_plus_one_as_sql_date}'
# GROUP BY eventName, eventCategory, userAgent
# ORDER BY event_count DESC
# """
#
#     print(sql_query)
#
#     # 1. Start the query
#     response = client.start_query(
#         QueryStatement=sql_query
#     )
#     query_id = response['QueryId']
#     print(f"Started Query: {query_id}")
#
#     # 2. Poll for results
#     while True:
#         status_response = client.describe_query(QueryId=query_id)
#         status = status_response['QueryStatus']
#         print(f"Status: {status}")
#
#         if status in ['FINISHED', 'FAILED', 'CANCELLED']:
#             break
#         time.sleep(5)  # Wait before polling again
#
#     # 3. Get the results if finished successfully
#     if status == 'FINISHED':
#         return get_all_results(client, query_id)
#
#     print(f"Query failed with status: {status}")
#
#     return pl.DataFrame()
#
#
#
#
#
#
#
#
#
