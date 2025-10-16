namespace DateTimeService.Application.Queries
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

        public const string TableGoodsRawCreatePreliminary = """
            CREATE TABLE #Temp_GoodsRaw   
            (	
                Article nvarchar(50), 
                Code nvarchar(11)
            )
            ;

            """;

        public const string TableGoodsRawInsertPreliminary = """
            INSERT INTO 
                #Temp_GoodsRaw ( 
                    Article, Code 
                )
            VALUES
                {0}
            OPTION (KEEP PLAN, KEEPFIXED PLAN)
            ;

            """;

        public const string PickupPointsQuery = """
            SELECT DISTINCT
            	T1._IDRRef ПунктВыдачи,
            	T1._Fld23620RRef ГрафикРаботы
            INTO #Temp_PickupPoints
            FROM 
            	dbo._Reference226 T1 WITH(NOLOCK)
            WHERE T1._Fld19544 IN ({0})
            OPTION (KEEP PLAN, KEEPFIXED PLAN)
            ;
            
            """;
    }
}
