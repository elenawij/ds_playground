#connect to to data base
mydb<-dbConnect(MySQL(), user='', password='', dbname='', host='')

#get data from data base
bike.data.query<-dbSendQuery(mydb,"select f.id as 'Trip ID', f.duration as Duration, CONVERT_TZ(date(f.start_date),'UTC','America/Los_Angeles') as 'Start Date', st.station_name as 'Start Station', st.terminal_name as 'Start Terminal',CONVERT_TZ(date(f.end_date),'UTC','America/Los_Angeles')  as 'End Date', st2.station_name as 'End Station', st2.terminal_name as 'End Terminal', e.entity_name as 'Bike #', e.entity_type as 'Subscriber Type', z.zip_code  as 'Zip Code' from fact_trips f 
                             join dim_stations st on f.start_station_id=st.id
                             join dim_stations st2 on f.end_station_id=st2.id
                             join dim_entity e on f.entity_id=e.id
                             join dim_entity_zip z on f.entity_id=z.entity_id")
bike.data<-fetch(bike.data.query, n=-1)
