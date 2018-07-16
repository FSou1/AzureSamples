ALTER TABLE [dbo].[Brands] ADD PRIMARY KEY (Id);

ALTER TABLE [dbo].[Products] ADD PRIMARY KEY (Id);

ALTER TABLE [dbo].[People] ADD PRIMARY KEY (Id);

ALTER TABLE [dbo].[Orders] ADD PRIMARY KEY (Id);

ALTER TABLE [dbo].[Products] ADD CONSTRAINT [FK_ProductBrand] FOREIGN KEY (BrandId) REFERENCES [dbo].[Brands] (Id) ON DELETE CASCADE;

ALTER TABLE [dbo].[Orders] ADD CONSTRAINT [FK_OrderPerson] FOREIGN KEY (PersonId) REFERENCES [dbo].[People] (Id) ON DELETE CASCADE;
ALTER TABLE [dbo].[Orders] ADD CONSTRAINT [FK_OrderProduct] FOREIGN KEY (ProductId) REFERENCES [dbo].[Products] (Id) ON DELETE CASCADE;