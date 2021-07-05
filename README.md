# EntityFrameworkCore.Manipulation.Extensions
Do you find yourself in a situation where you're in need of more advanced data manipulation operations than Entity Framework Core provides, 
such as Upsert or Merge/Sync? Then you've made it to the right place. This library providers extensions to Entity Framework core to support
transactional upserts, inserts if not exist, and full syncs/merges. Additionally it provides you with the result of those operations, allowing
you to see which entities were inserted, updated, and/or deleted.

**Note:** this library is currently in preview and the contract might change somewhat until the preview phase is completed.

## Supported Databases (Providers)

Currently, this library supports the following databases:
* SQL Server
* SQLite

## Getting Started

Install the latest version of the [EntityFrameworkCore.Manipulation.Extensions](https://www.nuget.org/packages/EntityFrameworkCore.Manipulation.Extensions)
through Nuget. No additional configuration is needed to get going, but there are several settings that can be tweaked to your needs (check them out below).

Here's an example of an _Upsert_ (i.e. insert or update) of entities that are part of your `DbContext`, and a print-out of which entities got inserted or updated.

```C#
var entitiesToUpsert = new[] 
{
  new MyEntity { Id = "AlreadyExists", Value = "This is the new value" },
  new MyEntity { Id = "NewEntity", Value = "This is the new value" },
};
  

using var dbContext = new MyContext(); // get or instantiate your DbContext
var upsertResult = await dbContext.UpsertAsync(entitiesToUpsert);

Console.WriteLine($"The following entities were inserted: [{string.Join(',', upsertResult.InsertedEntities.Select(x => x.Id))}]");
Console.WriteLine($"The following entities were updated: [{string.Join(',', upsertResult.UpdatedEntities.Select(x => x.NewValue.Id))}]");

// The following entities were inserted: [NewEntity]
// The following entities were updated: [AlreadyExists] 
```

## Contract 

TODO

## Configuration

TODO

