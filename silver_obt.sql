CREATE OR REFRESH STREAMING TABLE silver_obt
AS 
SELECT
    r.*



, vm.vehicle_make



, vt.vehicle_type

, vt.description

, vt.base_rate

, vt.per_mile

, vt.per_minute



, cr.cancellation_reason



, mc.city as pickup_city

, mc.state

, mc.region

, mc.updated_at AS pickup_city_updated_at


, pm.payment_method

, pm.is_card

, pm.requires_auth



, rs.ride_status



FROM  STREAM (uber.bronze.stg_rides) 
WATERMARK booking_timestamp AS watermark_ts DELAY OF INTERVAL 3 MINUTES r


LEFT JOIN uber.bronze.map_vehicle_makes vm
ON r.vehicle_make_id = vm.vehicle_make_id

LEFT JOIN uber.bronze.map_vehicle_types vt
ON r.vehicle_type_id = vt.vehicle_type_id

LEFT JOIN uber.bronze.map_cancellation_reasons cr
ON r.cancellation_reason_id = cr.cancellation_reason_id

LEFT JOIN uber.bronze.map_cities mc
ON r.pickup_city_id = mc.city_id

LEFT JOIN uber.bronze.map_payment_methods pm
ON r.payment_method_id = pm.payment_method_id

LEFT JOIN uber.bronze.map_ride_statuses rs
ON r.ride_status_id = rs.ride_status_id
