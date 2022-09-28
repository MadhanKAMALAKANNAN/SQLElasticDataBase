// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// <auto-generated />

namespace SQLElasticDataBase.Migrations
{
    using System.CodeDom.Compiler;
    using System.Data.Entity.Migrations;
    using System.Data.Entity.Migrations.Infrastructure;
    using System.Resources;
    
    [GeneratedCode("EntityFramework.Migrations", "6.1.0-30225")]
    public sealed partial class AddUser : IMigrationMetadata
    {
        private readonly ResourceManager Resources = new ResourceManager(typeof(AddUser));
        
        string IMigrationMetadata.Id
        {
            get { return "201405280638095_AddUser"; }
        }
        
        string IMigrationMetadata.Source
        {
            get { return null; }
        }
        
        string IMigrationMetadata.Target
        {
            get { return Resources.GetString("Target"); }
        }
    }
}
