# DatabricksTV

this example is to show how you can manipulate two dataframe and joining them, the same example that i made in Datafactory.
this example uses two datasets that were extracted using python from MediaMarkt Outlet website.

1. Import two csv files from Blob Container.
2. read data frame and make column based on some rules.
3. extract the productId from the link.
4. joining the tvinformation with tv damage details using df.join productID.
