

SELECT * FROM Orders
INNER JOIN Product
ON Orders.productId = Product.id

SELECT *
FROM
  Orders o,
  Shipments s
WHERE
  o.id = s.orderId AND
  s.shiptime BETWEEN o.ordertime AND o.ordertime + INTERVAL '4' HOUR
