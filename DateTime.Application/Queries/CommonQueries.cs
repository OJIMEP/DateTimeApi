namespace DateTime.Application.Queries
{
    public static class CommonQueries
    {
        public const string TableGoodsRawCreate = """
            Create Table #Temp_GoodsRaw   
            (	
                Article nvarchar(20), 
                code nvarchar(20), 
                PickupPoint nvarchar(150),
                quantity int 
            );
            """;

        public const string TableGoodsRawInsert = """
            INSERT INTO 
                #Temp_GoodsRaw ( 
                    Article, code, PickupPoint, quantity 
                )
            VALUES
                {0}
            OPTION (KEEP PLAN, KEEPFIXED PLAN);
            """;
    }
}
