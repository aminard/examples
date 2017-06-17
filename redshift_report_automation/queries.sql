total_visitors;
SELECT LOWER(client) AS client, DATE(dt), COUNT(DISTINCT bvid) AS total_visitors 
FROM pageview 
WHERE LOWER(client) = '{0}' 
AND dt BETWEEN '{1} 00:00:00' AND '{1} 23:59:59'
AND LOWER(bvproduct) = 'curations'
GROUP BY LOWER(client), DATE(dt) 
ORDER BY LOWER(client), DATE(dt);

total_orders;
SELECT LOWER(client) AS client, DATE(dt), COUNT(DISTINCT(loadid)) AS total_orders
FROM transaction 
WHERE LOWER(client) = '{0}' 
AND dt BETWEEN '{1} 00:00:00' AND '{1} 23:59:59'
AND bvid IN 
    (SELECT DISTINCT(bvid) 
    FROM pageview 
    WHERE LOWER(client) = '{0}' 
    AND dt BETWEEN '{1} 00:00:00' AND '{1} 23:59:59'
    AND LOWER(bvproduct) = 'curations') 
GROUP BY LOWER(client), DATE(dt) 
ORDER BY LOWER(client), DATE(dt);

total_revenue;
SELECT LOWER(client) AS client, orderdate AS date, SUM(ordertotal) AS total_revenue, currency AS total_currency
FROM 
    (SELECT client, DATE(dt) AS orderdate, loadid, ordertotal, currency 
    FROM transaction 
    WHERE LOWER(client) = '{0}' 
    AND dt BETWEEN '{1} 00:00:00' AND '{1} 23:59:59' 
    AND bvid IN 
        (SELECT DISTINCT(bvid) 
        FROM pageview 
        WHERE LOWER(client) = '{0}' 
        AND dt BETWEEN '{1} 00:00:00' AND '{1} 23:59:59'
        AND LOWER(bvproduct) = 'curations')
	GROUP BY client, DATE(dt), loadid, ordertotal, currency) 
GROUP BY LOWER(client), orderdate, currency 
ORDER BY LOWER(client), orderdate, currency;