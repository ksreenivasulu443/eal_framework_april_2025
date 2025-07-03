SELECT  a.customer_id, upper(a.name) as name,a.phone
FROM [dbo].[customers_raw] a inner join [dbo].[customers_bronze] b
on a.customer_id = b.customer_id
where a.customer_id = 105