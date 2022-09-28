// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Data.Entity.Migrations;

namespace SQLElasticDataBase.Migrations
{
    public sealed class Configuration : DbMigrationsConfiguration<SQLElasticDataBase.ElasticScaleContext<int>>
    {
        public Configuration()
        {
            this.AutomaticMigrationsEnabled = false;
            this.ContextKey = "CodeFirstNewDatabaseSample.BloggingContext";
        }

        protected override void Seed(SQLElasticDataBase.ElasticScaleContext<int> context)
        {
            // This method will be called after migrating to the latest version.

            // You can use the DbSet<T>.AddOrUpdate() helper extension method 
            // to avoid creating duplicate seed data. E.g.
            //
            // context.People.AddOrUpdate(
            //   p => p.FullName,
            //   new Person { FullName = "Andrew Peters" },
            //   new Person { FullName = "Brice Lambson" },
            //   new Person { FullName = "Rowan Miller" }
            // );
        }
    }
}
