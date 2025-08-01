﻿namespace DateTimeService.Application.Queries
{
    public static class AvailableDateQueries
    {

        //        public const string AvailableDate1 = @"
        //-- Шаг 1: Разбираем строку PickupPoint на отдельные пункты выдачи
        //WITH ParsedPickupPoints AS (
        //    SELECT 
        //        g.Article,
        //        g.PickupPoint AS OriginalPickupPoints,
        //        TRIM(s.value) AS SinglePickupPoint
        //    FROM 
        //        #Temp_GoodsRaw g
        //    CROSS APPLY 
        //        STRING_SPLIT(g.PickupPoint, ',') s
        //    WHERE 
        //        g.PickupPoint IS NOT NULL
        //),
        //-- Шаг 2: Считаем популярность каждого пункта выдачи (сколько Article на него ссылается)
        //PickupPointPopularity AS (
        //    SELECT 
        //		SinglePickupPoint,
        //        Склады._IDRRef AS СкладСсылка,
        //        COUNT(DISTINCT Article) AS Popularity
        //    FROM 
        //        ParsedPickupPoints
        //		INNER JOIN dbo._Reference226 Склады WITH (NOLOCK)
        //		ON SinglePickupPoint = Склады._Fld19544
        //    GROUP BY 
        //		SinglePickupPoint,
        //        Склады._IDRRef 
        //),
        //-- Шаг 3: Для каждого оригинального списка PickupPoint получаем все возможные пункты
        //OriginalPickupLists AS (
        //    SELECT DISTINCT
        //        OriginalPickupPoints
        //    FROM 
        //        ParsedPickupPoints
        //),
        //-- Шаг 4: Для каждого списка ранжируем пункты по популярности
        //RankedPickupPoints AS (
        //    SELECT 
        //        op.OriginalPickupPoints,
        //        pp.СкладСсылка,
        //		ROW_NUMBER() OVER(
        //            PARTITION BY op.OriginalPickupPoints 
        //            ORDER BY pp.Popularity DESC, pp.SinglePickupPoint
        //        ) AS RowNum
        //    FROM 
        //        OriginalPickupLists op
        //    CROSS APPLY (
        //        SELECT 
        //            p.SinglePickupPoint,
        //			pop.СкладСсылка,
        //            pop.Popularity
        //        FROM 
        //            ParsedPickupPoints p
        //            JOIN PickupPointPopularity pop ON p.SinglePickupPoint = pop.SinglePickupPoint
        //        WHERE 
        //            p.OriginalPickupPoints = op.OriginalPickupPoints
        //        GROUP BY
        //            p.SinglePickupPoint,
        //			pop.СкладСсылка,
        //            pop.Popularity
        //    ) pp
        //)

        //-- Шаг 5: Выбираем топ-5 самых популярных пунктов для каждого списка (по отдельным строкам)
        //SELECT 
        //    r.OriginalPickupPoints AS СписокПВЗ,
        //	r.СкладСсылка
        //INTO #Temp_ParsedPickupPoints
        //FROM 
        //    RankedPickupPoints r
        //WHERE 
        //    r.RowNum <= 5
        //OPTION (KEEP PLAN, KEEPFIXED PLAN)
        //;

        public const string AvailableDate1 = @"
WITH ParsedPickupPoints AS (
    SELECT DISTINCT g.PickupPoint As OriginalPickupPoints, s.value AS Code
    FROM (
        SELECT DISTINCT PickupPoint 
        FROM #Temp_GoodsRaw 
        WHERE PickupPoint IS NOT NULL
    ) AS g
    CROSS APPLY STRING_SPLIT(g.PickupPoint, ',') s
),
RankedWarehouses AS (
    SELECT
        pp.OriginalPickupPoints AS СписокПВЗ,
        Склады._IDRRef AS СкладСсылка,
        Склады._Fld19544 AS ERPКодСклада,
        ROW_NUMBER() OVER(PARTITION BY pp.OriginalPickupPoints ORDER BY Склады._IDRRef) AS RowNum
    FROM 
        dbo._Reference226 Склады WITH (NOLOCK)
        INNER JOIN ParsedPickupPoints pp ON Склады._Fld19544 = pp.Code
)
SELECT
    СписокПВЗ,
    СкладСсылка,
    ERPКодСклада
INTO #Temp_ParsedPickupPoints
FROM 
    RankedWarehouses
WHERE 
    RowNum <= {8}
ORDER BY 
    СписокПВЗ, ERPКодСклада
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

SELECT DISTINCT 
	T1.СкладСсылка
INTO #Temp_PickupPoints
FROM 
	#Temp_ParsedPickupPoints T1 WITH (NOLOCK)
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;
 
Select
	IsNull(_Reference114_VT23370._Fld23372RRef,Геозона._Fld23104RRef) As СкладСсылка,
	ЗоныДоставки._ParentIDRRef As ЗонаДоставкиРодительСсылка,
	Геозона._IDRRef As Геозона
Into #Temp_GeoData
From dbo._Reference114 Геозона With (NOLOCK)
	Inner Join _Reference114_VT23370 With (NOLOCK)
	on _Reference114_VT23370._Reference114_IDRRef = Геозона._IDRRef
	Inner Join _Reference99 ЗоныДоставки With (NOLOCK)
	on Геозона._Fld2847RRef = ЗоныДоставки._IDRRef
where Геозона._IDRRef IN (
	SELECT TOP 1
		T3._Fld26708RRef AS Fld26823RRef --геозона из рс векРасстоянияАВ
	FROM (SELECT
			T1._Fld25549 AS Fld25549_,
			MAX(T1._Period) AS MAXPERIOD_ 
		FROM dbo._InfoRg21711 T1 With (NOLOCK)
		WHERE T1._Fld26708RRef <> 0x00 and T1._Fld25549 = @P_CityCode
		GROUP BY T1._Fld25549) T2
	INNER JOIN dbo._InfoRg21711 T3 With (NOLOCK)
	ON T2.Fld25549_ = T3._Fld25549 AND T2.MAXPERIOD_ = T3._Period
	)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select Distinct
	Номенклатура._IDRRef AS НоменклатураСсылка,
	Номенклатура._Code AS code,
	Номенклатура._Fld3480 AS article,
	Номенклатура._Fld3489RRef AS ЕдиницаИзмерения,
	Номенклатура._Fld3526RRef AS Габариты,
	Упаковки._IDRRef AS УпаковкаСсылка,
	Упаковки._Fld6000 AS Вес,
	Упаковки._Fld6006 AS Объем,
	Номенклатура._Fld21822RRef as ТНВЭДСсылка,
    Номенклатура._Fld3515RRef as ТоварнаяКатегорияСсылка
INTO #Temp_GoodsPackages
From
	#Temp_GoodsRaw T1
	Inner Join 	dbo._Reference149 Номенклатура With (NOLOCK) 
		ON T1.code is NULL 
		And T1.PickupPoint is null
		And T1.Article = Номенклатура._Fld3480
	Inner Join dbo._Reference256 Упаковки With (NOLOCK)
		On 
		Упаковки._OwnerID_TYPE = 0x08  
		And Упаковки.[_OwnerID_RTRef] = 0x00000095
		And Номенклатура._IDRRef = Упаковки._OwnerID_RRRef		
		And Упаковки._Fld6003RRef = Номенклатура._Fld3489RRef
		And Упаковки._Marked = 0x00
Union all
Select Distinct
	Номенклатура._IDRRef,
	Номенклатура._Code,
	Номенклатура._Fld3480,
	Номенклатура._Fld3489RRef,
	Номенклатура._Fld3526RRef,
	Упаковки._IDRRef AS УпаковкаСсылка,
	Упаковки._Fld6000 AS Вес,
	Упаковки._Fld6006 AS Объем,
    Номенклатура._Fld21822RRef as ТНВЭДСсылка,
    Номенклатура._Fld3515RRef as ТоварнаяКатегорияСсылка
From 
	#Temp_GoodsRaw T1
	Inner Join 	dbo._Reference149 Номенклатура With (NOLOCK) 
		ON T1.code is not NULL 
		And T1.PickupPoint is null
		And T1.code = Номенклатура._Code
	Inner Join dbo._Reference256 Упаковки With (NOLOCK)
		On 
		Упаковки._OwnerID_TYPE = 0x08  
		And Упаковки.[_OwnerID_RTRef] = 0x00000095
		And Номенклатура._IDRRef = Упаковки._OwnerID_RRRef		
		And Упаковки._Fld6003RRef = Номенклатура._Fld3489RRef
		And Упаковки._Marked = 0x00
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

SELECT 
    Номенклатура.НоменклатураСсылка AS НоменклатураСсылка,
    Номенклатура.code AS code,
    Номенклатура.article AS article,
    Номенклатура.ЕдиницаИзмерения AS ЕдиницаИзмерения,
    Номенклатура.Габариты AS Габариты,
    #Temp_ParsedPickupPoints.СкладСсылка AS СкладПВЗСсылка,
    Номенклатура.УпаковкаСсылка AS УпаковкаСсылка,
    Номенклатура.Вес AS Вес,
    Номенклатура.Объем AS Объем,
    Номенклатура.ТНВЭДСсылка AS ТНВЭДСсылка,
    Номенклатура.ТоварнаяКатегорияСсылка AS ТоварнаяКатегорияСсылка
INTO 
    #Temp_GoodsBegin
FROM
    #Temp_GoodsRaw T1
    INNER JOIN #Temp_GoodsPackages Номенклатура
    ON T1.code IS NULL 
        AND T1.Article = Номенклатура.article
    LEFT JOIN #Temp_ParsedPickupPoints  
        ON T1.PickupPoint = #Temp_ParsedPickupPoints.СписокПВЗ

UNION ALL

SELECT 
    Номенклатура.НоменклатураСсылка,
    Номенклатура.code,
    Номенклатура.article,
    Номенклатура.ЕдиницаИзмерения,
    Номенклатура.Габариты,
    #Temp_ParsedPickupPoints.СкладСсылка,
    Номенклатура.УпаковкаСсылка AS УпаковкаСсылка,
    Номенклатура.Вес AS Вес,
    Номенклатура.Объем AS Объем,
    Номенклатура.ТНВЭДСсылка AS ТНВЭДСсылка,
    Номенклатура.ТоварнаяКатегорияСсылка AS ТоварнаяКатегорияСсылка
FROM 
    #Temp_GoodsRaw T1
    INNER JOIN #Temp_GoodsPackages Номенклатура WITH (NOLOCK) 
    ON T1.code IS NOT NULL 
        AND T1.code = Номенклатура.code
    LEFT JOIN #Temp_ParsedPickupPoints
	ON T1.PickupPoint = #Temp_ParsedPickupPoints.СписокПВЗ
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

Select 
	Номенклатура.НоменклатураСсылка AS НоменклатураСсылка,
	Номенклатура.article AS article,
	Номенклатура.code AS code,
	Номенклатура.СкладПВЗСсылка AS СкладСсылка,
	Номенклатура.УпаковкаСсылка AS УпаковкаСсылка,--Упаковки._IDRRef AS УпаковкаСсылка,
	1 As Количество,
	Номенклатура.Вес AS Вес,--Упаковки._Fld6000 AS Вес,
	Номенклатура.Объем AS Объем,--Упаковки._Fld6006 AS Объем,
    Номенклатура.ТНВЭДСсылка as ТНВЭДСсылка,
    Номенклатура.ТоварнаяКатегорияСсылка as ТоварнаяКатегорияСсылка,
	10 AS ВремяНаОбслуживание,
	IsNull(ГруппыПланирования._IDRRef, 0x00000000000000000000000000000000) AS ГруппаПланирования,
	IsNull(ГруппыПланирования._IDRRef, 0x00000000000000000000000000000000) AS ОсновнаяГруппаПланирования,
	IsNull(ГруппыПланирования._Description, '') AS ГруппаПланированияНаименование,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(ГруппыПланирования._Fld25519, @P_EmptyDate)) AS ВремяНаПодготовкуПоставщик,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(ГруппыПланирования._Fld25520, @P_EmptyDate)) AS ВремяНаПодготовкуПеремещение,
	IsNull(ГруппыПланирования._Fld23302RRef, 0x00000000000000000000000000000000) AS ГруппаПланированияСклад,
	1 AS Приоритет
INTO #Temp_Goods
From 
	#Temp_GoodsBegin Номенклатура
	Left Join dbo._Reference23294 ГруппыПланирования With (NOLOCK)
		Inner Join dbo._Reference23294_VT23309 With (NOLOCK)
			on ГруппыПланирования._IDRRef = _Reference23294_VT23309._Reference23294_IDRRef
			and _Reference23294_VT23309._Fld23311RRef in (Select ЗонаДоставкиРодительСсылка From #Temp_GeoData)
		On 
		ГруппыПланирования._Fld23302RRef IN (Select СкладСсылка From #Temp_GeoData) --склад
		AND ГруппыПланирования._Fld25141 = 0x01--участвует в расчете мощности
		AND (ГруппыПланирования._Fld23301RRef = Номенклатура.Габариты OR (Номенклатура.Габариты = 0xAC2CBF86E693F63444670FFEB70264EE AND ГруппыПланирования._Fld23301RRef= 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D) ) --габариты
		AND ГруппыПланирования._Marked = 0x00
        AND Номенклатура.СкладПВЗСсылка Is Null
UNION ALL
Select 
	Номенклатура.НоменклатураСсылка AS НоменклатураСсылка,
	Номенклатура.article AS article,
	Номенклатура.code AS code,
	Номенклатура.СкладПВЗСсылка AS СкладСсылка,
	Номенклатура.УпаковкаСсылка AS УпаковкаСсылка,--Упаковки._IDRRef AS УпаковкаСсылка,
	1 As Количество,
	Номенклатура.Вес AS Вес,--Упаковки._Fld6000 AS Вес,
	Номенклатура.Объем AS Объем,--Упаковки._Fld6006 AS Объем,
    Номенклатура.ТНВЭДСсылка as ТНВЭДСсылка,
    Номенклатура.ТоварнаяКатегорияСсылка as ТоварнаяКатегорияСсылка,
	10 AS ВремяНаОбслуживание,
	IsNull(ПодчиненнаяГП._IDRRef, 0x00000000000000000000000000000000) AS ГруппаПланирования,
	IsNull(ГруппыПланирования._IDRRef, 0x00000000000000000000000000000000) AS ОсновнаяГруппаПланирования,
	IsNull(ПодчиненнаяГП._Description, '') AS ГруппаПланированияНаименование,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(ПодчиненнаяГП._Fld25519, @P_EmptyDate)),
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(ПодчиненнаяГП._Fld25520, @P_EmptyDate)),
	IsNull(ПодчиненнаяГП._Fld23302RRef, 0x00000000000000000000000000000000) AS ГруппаПланированияСклад,
	0
From 
	#Temp_GoodsBegin Номенклатура
	Left Join dbo._Reference23294 ГруппыПланирования With (NOLOCK)
		Inner Join dbo._Reference23294_VT23309 With (NOLOCK)
			on ГруппыПланирования._IDRRef = _Reference23294_VT23309._Reference23294_IDRRef
			and _Reference23294_VT23309._Fld23311RRef in (Select ЗонаДоставкиРодительСсылка From #Temp_GeoData)
		On 
		ГруппыПланирования._Fld23302RRef IN (Select СкладСсылка From #Temp_GeoData) --склад
		AND ГруппыПланирования._Fld25141 = 0x01--участвует в расчете мощности
		AND (ГруппыПланирования._Fld23301RRef = Номенклатура.Габариты OR (Номенклатура.Габариты = 0xAC2CBF86E693F63444670FFEB70264EE AND ГруппыПланирования._Fld23301RRef= 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D) ) --габариты
		AND ГруппыПланирования._Marked = 0x00
		AND Номенклатура.СкладПВЗСсылка Is Null
	Inner Join dbo._Reference23294 ПодчиненнаяГП WITH(NOLOCK)
			On  ГруппыПланирования._Fld26526RRef = ПодчиненнаяГП._IDRRef
Where 
	Номенклатура.СкладПВЗСсылка IS NULL
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select 
	ЦеныТолькоПрайсы._Fld21408RRef AS Номенклатура,
	ЦеныТолькоПрайсы._Fld21410_TYPE AS Регистратор_TYPE,
    ЦеныТолькоПрайсы._Fld21410_RTRef AS Регистратор_RTRef,
    ЦеныТолькоПрайсы._Fld21410_RRRef AS Регистратор_RRRef
Into #Temp_Megaprices
From _AccumRg21407 ЦеныТолькоПрайсы With (NOLOCK)
	Inner Join _InfoRgSL26678 ExchangeRates With (NOLOCK)
		ON ЦеныТолькоПрайсы._Fld21443RRef = ExchangeRates._Fld14558RRef
		AND ЦеныТолькоПрайсы._Fld21410_RTRef = 0x00000153 --Цены.Регистратор ССЫЛКА Документ.мегапрайсРегистрацияПрайса
        And ЦеныТолькоПрайсы._Fld30969 = 0x00 -- ОтказПоФильтруЦен = Ложь
		--AND (ЦеныТолькоПрайсы._Fld21982 <> 0 
			--AND ЦеныТолькоПрайсы._Fld21442 * ExchangeRates._Fld14559 / ExchangeRates._Fld14560 >= ЦеныТолькоПрайсы._Fld21982 OR ЦеныТолькоПрайсы._Fld21411 >= ЦеныТолькоПрайсы._Fld21616)
		And ЦеныТолькоПрайсы._Fld21408RRef IN(SELECT
                НоменклатураСсылка
            FROM
                #Temp_GoodsPackages)
OPTION (KEEP PLAN, KEEPFIXED PLAN);
				
Select Distinct
	Цены._Fld21408RRef AS Номенклатура,
	Цены._Fld21410_RRRef AS РегистраторЦен_RRRef,
	Цены._Fld21442 * ExchangeRates._Fld14559 / ExchangeRates._Fld14560 AS Цена
Into #Temp_Prices
From _AccumRg21407 Цены With (NOLOCK)
	Inner Join _InfoRgSL26678 ExchangeRates With (NOLOCK)
		ON Цены._Fld21443RRef = ExchangeRates._Fld14558RRef
		AND Цены._Fld21410_RTRef IN(0x00000141,0x00000153)  --Регистратор ССЫЛКА Документ.мегапрайсРегистрацияПрайса, ЗаказПоставщику
		AND (Цены._Fld21982 <> 0 
			AND Цены._Fld21410_RTRef = 0x00000141 OR (Цены._Fld21442 * ExchangeRates._Fld14559 / ExchangeRates._Fld14560 >= Цены._Fld21982 OR Цены._Fld21411 >= Цены._Fld21616))
		AND Цены._Fld21408RRef IN(SELECT
                НоменклатураСсылка
            FROM
                #Temp_GoodsPackages)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

WITH FilteredGoods AS (
    SELECT
        TNomen.НоменклатураСсылка
    FROM
        #Temp_GoodsPackages TNomen WITH(NOLOCK)
)
SELECT
    T2._Fld21408RRef AS НоменклатураСсылка,
    T2._Fld21410_TYPE AS Источник_TYPE,
    T2._Fld21410_RTRef AS Источник_RTRef,
    T2._Fld21410_RRRef AS Источник_RRRef,
    #Temp_Megaprices.Регистратор_TYPE AS Регистратор_TYPE,
    #Temp_Megaprices.Регистратор_RTRef AS Регистратор_RTRef,
    #Temp_Megaprices.Регистратор_RRRef AS Регистратор_RRRef,
    T2._Fld23568RRef AS СкладИсточника,
    T2._Fld21424 AS ДатаСобытия,
    #Temp_Prices.Цена AS Цена,
    SUM(T2._Fld21411) - SUM(T2._Fld21412) AS Количество
Into #Temp_Remains
FROM
    dbo._AccumRgT21444 T2 WITH (NOLOCK)
	INNER JOIN FilteredGoods FG 
	ON T2._Fld21408RRef = FG.НоменклатураСсылка
		AND T2._Period = '5999-11-01 00:00:00'
		AND (T2._Fld21424 = '2001-01-01 00:00:00'
			OR (T2._Fld21424 >= @P_DateTimeNow))
		AND (T2._Fld21412 <> 0 OR T2._Fld21411 <> 0)
	LEFT JOIN #Temp_Megaprices
	ON T2._Fld21408RRef = #Temp_Megaprices.Номенклатура
	AND T2._Fld21410_RRRef = #Temp_Megaprices.Регистратор_RRRef
    LEFT JOIN #Temp_Prices
	ON T2._Fld21408RRef = #Temp_Prices.Номенклатура
	AND T2._Fld21410_RRRef = #Temp_Prices.РегистраторЦен_RRRef
	
GROUP BY
    T2._Fld21408RRef,
    T2._Fld21410_TYPE,
    T2._Fld21410_RTRef,
    T2._Fld21410_RRRef,
    #Temp_Megaprices.Регистратор_TYPE,
	#Temp_Megaprices.Регистратор_RTRef,
	#Temp_Megaprices.Регистратор_RRRef,
    T2._Fld23568RRef,
    T2._Fld21424,
	#Temp_Prices.Цена
HAVING
    (SUM(T2._Fld21412) <> 0.0
    OR SUM(T2._Fld21411) <> 0.0)
    AND SUM(T2._Fld21411) - SUM(T2._Fld21412) > 0.0
OPTION (HASH GROUP, OPTIMIZE FOR (@P_DateTimeNow='{1}'),KEEP PLAN, KEEPFIXED PLAN);

WITH Склады AS (
	SELECT 
		СкладСсылка 
	FROM #Temp_GeoData 

	UNION ALL 

	SELECT 
		СкладСсылка
	FROM #Temp_PickupPoints
),
SourceStocks AS (
    SELECT DISTINCT
        T1.СкладИсточника,
		T1.ДатаСобытия
    FROM #Temp_Remains T1 WITH (NOLOCK)
	WHERE T1.ДатаСобытия <> '2001-01-01 00:00:00'
)
SELECT
    ПрогнозныеДатыПоставокНаСклады._Fld23831RRef AS СкладИсточника,
    ПрогнозныеДатыПоставокНаСклады._Fld23832 AS ДатаСобытия,
    ПрогнозныеДатыПоставокНаСклады._Fld23834 AS ДатаПрибытия,
    ПрогнозныеДатыПоставокНаСклады._Fld23833RRef AS СкладНазначения
INTO #Temp_WarehouseDates
FROM dbo._InfoRg23830 ПрогнозныеДатыПоставокНаСклады WITH (NOLOCK)
	INNER JOIN SourceStocks 
    ON ПрогнозныеДатыПоставокНаСклады._Fld23831RRef = SourceStocks.СкладИсточника
		AND ПрогнозныеДатыПоставокНаСклады._Fld23832 = SourceStocks.ДатаСобытия
	INNER JOIN Склады WITH (NOLOCK)
    ON ПрогнозныеДатыПоставокНаСклады._Fld23833RRef = Склады.СкладСсылка
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;";

        public const string AvailableDate2MinimumWarehousesBasic = @"With SourceWarehouses AS
(
SELECT Distinct
	T2.СкладИсточника AS СкладИсточника
FROM
	#Temp_Remains T2 WITH(NOLOCK)
)
SELECT
	T1._Fld23831RRef AS СкладИсточника,
	T1._Fld23833RRef AS СкладНазначения,
	MIN(T1._Fld23834) AS ДатаПрибытия 
Into #Temp_MinimumWarehouseDates
FROM
    dbo._InfoRg23830 T1 With (NOLOCK{7})
    Inner Join SourceWarehouses On T1._Fld23831RRef = SourceWarehouses.СкладИсточника
WHERE
    T1._Fld23833RRef IN (Select СкладСсылка From #Temp_GeoData UNION ALL Select СкладСсылка From #Temp_PickupPoints)
		AND	T1._Fld23832 BETWEEN @P_DateTimeNow AND @P_DateTimePeriodEnd --DateAdd(DAY,6,@P_DateTimeNow)
GROUP BY T1._Fld23831RRef,
T1._Fld23833RRef
OPTION (HASH GROUP, OPTIMIZE FOR (@P_DateTimeNow='{1}'), KEEP PLAN, KEEPFIXED PLAN);";
        public const string AvailableDate2MinimumWarehousesCustom = @"With SourceWarehouses AS
(
SELECT Distinct
	T2.СкладИсточника AS СкладИсточника
FROM
	#Temp_Remains T2 WITH(NOLOCK)
),
Stocks AS (Select СкладСсылка From #Temp_GeoData UNION ALL Select СкладСсылка From #Temp_PickupPoints)
SELECT
	T1.СкладИсточника AS СкладИсточника,
	T1.СкладНазначения AS СкладНазначения,
	MIN(T1.ДатаПрибытия) AS ДатаПрибытия  
Into #Temp_MinimumWarehouseDates
FROM
    [dbo].[WarehouseDatesAggregate] T1 With (NOLOCK{7})
    Inner Join SourceWarehouses On T1.СкладИсточника = SourceWarehouses.СкладИсточника
	Inner Join Stocks ON Stocks.СкладСсылка = T1.СкладНазначения
WHERE
    --T1.СкладНазначения IN (Select СкладСсылка From #Temp_GeoData UNION ALL Select СкладСсылка From #Temp_PickupPoints)
	--AND	
	T1.ДатаСобытия BETWEEN @P_DateTimeNow AND @P_DateTimePeriodEnd --DateAdd(DAY,6,@P_DateTimeNow)
GROUP BY T1.СкладИсточника,
T1.СкладНазначения
OPTION (OPTIMIZE FOR (@P_DateTimeNow='{1}'), KEEP PLAN, KEEPFIXED PLAN);";

        public const string AvailableDate3 = @"SELECT
    T1.НоменклатураСсылка,
    T1.Количество,
    T1.Источник_TYPE,
    T1.Источник_RTRef,
    T1.Источник_RRRef,
    T1.СкладИсточника,
    T1.Цена,
    T1.ДатаСобытия,
    ISNULL(T3.ДатаПрибытия, T2.ДатаПрибытия) AS ДатаДоступности,
    1 AS ТипИсточника,
    ISNULL(T3.СкладНазначения, T2.СкладНазначения) AS СкладНазначения
INTO #Temp_Sources
FROM
    #Temp_Remains T1 WITH(NOLOCK)
    LEFT OUTER JOIN #Temp_WarehouseDates T2 WITH(NOLOCK)
    ON (T1.СкладИсточника = T2.СкладИсточника)
    AND (T1.ДатаСобытия = T2.ДатаСобытия)
    LEFT OUTER JOIN #Temp_MinimumWarehouseDates T3 WITH(NOLOCK)
    ON (T1.СкладИсточника = T3.СкладИсточника)
    AND (T1.ДатаСобытия = '2001-01-01 00:00:00')
WHERE
    T1.Источник_RTRef = 0x000000E2 OR T1.Источник_RTRef = 0x00000150

UNION
ALL
SELECT
    T4.НоменклатураСсылка,
    T4.Количество,
    T4.Источник_TYPE,
    T4.Источник_RTRef,
    T4.Источник_RRRef,
    T4.СкладИсточника,
    T4.Цена,
    T4.ДатаСобытия,
    T5.ДатаПрибытия,
    2,
    T5.СкладНазначения
FROM
    #Temp_Remains T4 WITH(NOLOCK)
    INNER JOIN #Temp_WarehouseDates T5 WITH(NOLOCK)
    ON (T4.СкладИсточника = T5.СкладИсточника)
    AND (T4.ДатаСобытия = T5.ДатаСобытия)
WHERE
    T4.Источник_RTRef = 0x00000141

UNION
ALL
SELECT
    T6.НоменклатураСсылка,
    T6.Количество,
    T6.Источник_TYPE,
    T6.Источник_RTRef,
    T6.Источник_RRRef,
    T6.СкладИсточника,
    T6.Цена,
    T6.ДатаСобытия,
    T7.ДатаПрибытия,
    3,
    T7.СкладНазначения
FROM
    #Temp_Remains T6 WITH(NOLOCK)
    INNER JOIN #Temp_WarehouseDates T7 WITH(NOLOCK)
    ON (T6.СкладИсточника = T7.СкладИсточника)
    AND (T6.ДатаСобытия = T7.ДатаСобытия)
WHERE
    NOT T6.Регистратор_RRRef IS NULL
	And T6.Источник_RTRef = 0x00000153
OPTION (KEEP PLAN, KEEPFIXED PLAN);";

        public const string AvailableDate4SourcesWithPrices = @"
SELECT
    T1.НоменклатураСсылка,
    T1.Источник_TYPE,
    T1.Источник_RTRef,
    T1.Источник_RRRef,
    T1.СкладИсточника,
    T1.СкладНазначения,
    T1.ДатаСобытия,
    T1.ДатаДоступности,
	T1.Цена AS Цена
Into #Temp_SourcesWithPrices
FROM
    #Temp_Sources T1 WITH(NOLOCK)
Where  T1.Цена <> 0
OPTION (KEEP PLAN, KEEPFIXED PLAN, maxdop 2);";

        public const string PickupDateShift = @"
Select
	СмещениеДатДоставки._Fld25220RRef As Склад,
	СмещениеДатДоставки._Fld25221 As РазмерСмещения
Into #Temp_PickupDateShift
From dbo._InfoRg25217 СмещениеДатДоставки With (NOLOCK)
	Inner Join #Temp_PickupPoints PickupPoints
	On PickupPoints.СкладСсылка = СмещениеДатДоставки._Fld25220RRef
	And СмещениеДатДоставки._Fld25218 <= @P_DateTimeNow
	And СмещениеДатДоставки._Fld25219 >= @P_DateTimeNow
OPTION (KEEP PLAN, KEEPFIXED PLAN);";

        public const string AvailableDate5 = @"

With Temp_SupplyDocs AS
(
SELECT
    T1.НоменклатураСсылка,
    T1.СкладНазначения,
    T1.ДатаДоступности,
    DATEADD(DAY, {4}, T1.ДатаДоступности) AS ДатаДоступностиПлюс, --это параметр КоличествоДнейАнализа
    MIN(T1.Цена) AS ЦенаИсточника,
    MIN(T1.Цена / 100.0 * (100 - {5})) AS ЦенаИсточникаМинус --это параметр ПроцентДнейАнализа
FROM
    #Temp_SourcesWithPrices T1 WITH(NOLOCK)
WHERE 
    T1.Источник_RTRef = 0x00000153    
GROUP BY
    T1.НоменклатураСсылка,
    T1.ДатаДоступности,
    T1.СкладНазначения,
    DATEADD(DAY, {4}, T1.ДатаДоступности)
)
SELECT
    T2.НоменклатураСсылка,
    T2.ДатаДоступности,
    T2.СкладНазначения,
    T2.ДатаДоступностиПлюс,
    T2.ЦенаИсточника,
    T2.ЦенаИсточникаМинус,
    MIN(T1.ДатаДоступности) AS ДатаДоступности1,
    MIN(T1.Цена) AS Цена1
Into #Temp_BestPriceAfterClosestDate
FROM
    #Temp_SourcesWithPrices T1 WITH(NOLOCK)
    INNER HASH JOIN Temp_SupplyDocs T2 WITH(NOLOCK)
    ON (T1.НоменклатураСсылка = T2.НоменклатураСсылка)
    AND (T1.ДатаДоступности >= T2.ДатаДоступности)
    AND (T1.ДатаДоступности <= T2.ДатаДоступностиПлюс)
    AND (T1.Цена <= T2.ЦенаИсточникаМинус)
GROUP BY
    T2.НоменклатураСсылка,
    T2.СкладНазначения,
    T2.ДатаДоступностиПлюс,
    T2.ЦенаИсточника,
    T2.ЦенаИсточникаМинус,
    T2.ДатаДоступности
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);

With Temp_ClosestDate AS
(SELECT
T1.НоменклатураСсылка,
T1.СкладНазначения,
Cast(MIN(T1.ДатаДоступности)as datetime) AS ДатаДоступности
FROM #Temp_Sources T1 WITH(NOLOCK)
GROUP BY T1.НоменклатураСсылка,
T1.СкладНазначения
)
SELECT
    T4.НоменклатураСсылка,
    Min(T4.ДатаДоступности)AS ДатаДоступности,
    T4.СкладНазначения
Into #Temp_T3
FROM
    #Temp_Sources T4 WITH(NOLOCK)
    INNER JOIN Temp_ClosestDate T5 WITH(NOLOCK)
    ON (T4.НоменклатураСсылка = T5.НоменклатураСсылка)
		AND (T4.СкладНазначения = T5.СкладНазначения)
		AND (T4.ТипИсточника = 1)
		AND T4.ДатаДоступности <= DATEADD(DAY, {4}, T5.ДатаДоступности)
Group by T4.НоменклатураСсылка, T4.СкладНазначения
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);


With Temp_SourcesCorrectedDate AS
(
SELECT
    T1.НоменклатураСсылка,
    T1.СкладНазначения,
    Min(ISNULL(T2.ДатаДоступности1, T1.ДатаДоступности)) AS ДатаДоступности
FROM
    #Temp_Sources T1 WITH(NOLOCK)
    LEFT OUTER JOIN #Temp_BestPriceAfterClosestDate T2 WITH(NOLOCK)
    ON (T1.НоменклатураСсылка = T2.НоменклатураСсылка)
    AND (T1.ДатаДоступности = T2.ДатаДоступности)
    AND (T1.СкладНазначения = T2.СкладНазначения)
    AND (T1.ТипИсточника = 3)
GROUP BY
	T1.НоменклатураСсылка,
	T1.СкладНазначения
)
SELECT
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
    ISNULL(T3.СкладНазначения, T2.СкладНазначения) AS СкладНазначения,
    MIN(ISNULL(T3.ДатаДоступности, T2.ДатаДоступности)) AS БлижайшаяДата,
    1 AS Количество,
    T1.Вес,
    T1.Объем,
    T1.ТНВЭДСсылка,
    T1.ТоварнаяКатегорияСсылка,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
	T1.ВремяНаПодготовкуПоставщик,
	T1.ВремяНаПодготовкуПеремещение,
    T1.Приоритет,
	0 AS PickUp
into #Temp_ClosestDatesByGoodsWithoutShifting
FROM
    #Temp_Goods T1 WITH(NOLOCK)	
    LEFT JOIN Temp_SourcesCorrectedDate T2 WITH(NOLOCK)
		LEFT JOIN  #Temp_T3 T3 ON (T2.НоменклатураСсылка = T3.НоменклатураСсылка) 
			And T2.СкладНазначения = T3.СкладНазначения
    ON (T1.НоменклатураСсылка = T2.НоменклатураСсылка) 
		AND ISNULL(T3.СкладНазначения, T2.СкладНазначения) IN (Select СкладСсылка From #Temp_GeoData) 
Where 
	T1.СкладСсылка IS NULL
    And T1.ГруппаПланированияСклад = ISNULL(T3.СкладНазначения, T2.СкладНазначения)
GROUP BY
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
	ISNULL(T3.СкладНазначения, T2.СкладНазначения),
    T1.Вес,
    T1.Объем,
    T1.ТНВЭДСсылка,
    T1.ТоварнаяКатегорияСсылка,
    T1.ВремяНаОбслуживание,
    T1.Количество,
    T1.ГруппаПланирования,
	T1.ВремяНаПодготовкуПоставщик,
	T1.ВремяНаПодготовкуПеремещение,
	T1.Приоритет
UNION ALL
SELECT
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
    ISNULL(T3.СкладНазначения, T2.СкладНазначения) AS СкладНазначения,
    MIN(ISNULL(T3.ДатаДоступности, T2.ДатаДоступности)) AS БлижайшаяДата,
    1 AS Количество,
    T1.Вес,
    T1.Объем,
    T1.ТНВЭДСсылка,
    T1.ТоварнаяКатегорияСсылка,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
	T1.ВремяНаПодготовкуПоставщик,
	T1.ВремяНаПодготовкуПеремещение,
    T1.Приоритет,
	1 AS PickUp
FROM
    #Temp_Goods T1 WITH(NOLOCK)	
    LEFT JOIN Temp_SourcesCorrectedDate T2 WITH(NOLOCK)
		LEFT JOIN  #Temp_T3 T3 ON (T2.НоменклатураСсылка = T3.НоменклатураСсылка) 
			And T2.СкладНазначения = T3.СкладНазначения
    ON (T1.НоменклатураСсылка = T2.НоменклатураСсылка) 
		AND T1.СкладСсылка = ISNULL(T3.СкладНазначения, T2.СкладНазначения)
Where 
	NOT T1.СкладСсылка IS NULL
GROUP BY
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
	ISNULL(T3.СкладНазначения, T2.СкладНазначения),
    T1.Вес,
    T1.Объем,
    T1.ТНВЭДСсылка,
    T1.ТоварнаяКатегорияСсылка,
    T1.ВремяНаОбслуживание,
    T1.Количество,
    T1.ГруппаПланирования,
	T1.ВремяНаПодготовкуПоставщик,
	T1.ВремяНаПодготовкуПеремещение,
	T1.Приоритет
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
    T1.СкладНазначения,
    Case when ПрослеживаемыеТоварныеКатегории._Fld28349RRef is null then T1.БлижайшаяДата else DateAdd(DAY, @P_DaysToShift, ПрослеживаемыеТоварныеКатегории._Period) end as БлижайшаяДата,
    T1.Количество,
    T1.Вес,
    T1.Объем,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
	T1.ВремяНаПодготовкуПоставщик,
	T1.ВремяНаПодготовкуПеремещение,
    T1.Приоритет,
	T1.PickUp
into #Temp_ClosestDatesByGoods
FROM
    #Temp_ClosestDatesByGoodsWithoutShifting T1 WITH(NOLOCK)
	left join dbo._InfoRg28348 as ПрослеживаемыеТоварныеКатегории WITH(NOLOCK)
		on 1 = @P_ApplyShifting 
			and ПрослеживаемыеТоварныеКатегории._Fld28349RRef = T1.ТоварнаяКатегорияСсылка 
			and T1.БлижайшаяДата BETWEEN ПрослеживаемыеТоварныеКатегории._Period AND DateAdd(DAY, @P_DaysToShift, ПрослеживаемыеТоварныеКатегории._Period)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T1.НоменклатураСсылка,
    T1.article,
    T1.code,
    T1.СкладНазначения,
    T1.Вес,
    T1.Объем,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
    MIN(
        CASE
			WHEN T1.СкладНазначения = T2.СкладИсточника
				AND (T2.Источник_RTRef = 0x00000141
					OR T2.Источник_RTRef = 0x00000153)
                THEN DATEADD(SECOND, T1.ВремяНаПодготовкуПоставщик, T1.БлижайшаяДата)
			WHEN T1.СкладНазначения <> T2.СкладИсточника
				THEN DATEADD(SECOND, T1.ВремяНаПодготовкуПеремещение, T1.БлижайшаяДата)
            ELSE T1.БлижайшаяДата
        END
    ) AS ДатаДоступности,
    T1.Приоритет,
	T1.PickUp
Into #Temp_ShipmentDates
FROM
    #Temp_ClosestDatesByGoods T1 WITH(NOLOCK)
    LEFT OUTER JOIN #Temp_Sources T2 WITH(NOLOCK)
    ON (T1.НоменклатураСсылка = T2.НоменклатураСсылка)
    AND (T1.СкладНазначения = T2.СкладНазначения)
    AND (T1.БлижайшаяДата = T2.ДатаДоступности)
Where 
	NOT T1.БлижайшаяДата IS NULL
GROUP BY
    T1.НоменклатураСсылка,
    T1.article,
    T1.code,
    T1.СкладНазначения,
    T1.Вес,
    T1.Объем,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
    T1.Приоритет,
	T1.PickUp
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With MinDates AS
(
Select 
	T1.НоменклатураСсылка,
    MIN(T1.ДатаДоступности) AS ДатаСоСклада
FROM 
    #Temp_ShipmentDates T1 WITH(NOLOCK)
Where T1.PickUp = 0
Group by T1.НоменклатураСсылка
)
SELECT
    T1.НоменклатураСсылка,
    T1.article,
    T1.code,
    MinDates.ДатаСоСклада AS ДатаСоСклада,
    T1.Вес,
    T1.Объем,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
    T1.Приоритет,
	T1.PickUp
Into #Temp_ShipmentDatesDeliveryCourier
FROM
    #Temp_ShipmentDates T1 WITH(NOLOCK)
    Inner Join MinDates
		On T1.НоменклатураСсылка = MinDates.НоменклатураСсылка 
		And T1.ДатаДоступности = MinDates.ДатаСоСклада 
Where T1.PickUp = 0
OPTION (KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
    MIN(DATEADD(HOUR, ISNULL(СмещениеДатДоставки.РазмерСмещения, 0), T1.ДатаДоступности)) AS ДатаСоСклада,
	T1.СкладНазначения
Into #Temp_ShipmentDatesPickUp
FROM 
    #Temp_ShipmentDates T1 WITH(NOLOCK)
	LEFT OUTER JOIN #Temp_PickupDateShift СмещениеДатДоставки
	On СмещениеДатДоставки.Склад = T1.СкладНазначения
	And T1.ДатаДоступности < DATEADD(DAY, 2, @P_DateTimePeriodBegin) -- дата меньше чем конец завтрашнего дня
Where T1.PickUp = 1
GROUP BY
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
	T1.СкладНазначения
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Это получение списка дат интервалов ПВЗ*/
WITH
    H1(N)
    AS
    (
        SELECT 1
        FROM (VALUES
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1))H0(N)
    )
,
    cteTALLY(N)
    AS
    (
        SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
        FROM H1 a, H1 b, H1 c, H1 d, H1 e, H1 f, H1 g, H1 h
    ),
	Temp_PickupDatesGroup AS
	(
	Select 
		CAST(CAST(DateAdd(DAY, @P_DaysToShow,Max(ДатаСоСклада))AS date) AS datetime) AS МаксимальнаяДата,
		CAST(CAST(Min(ДатаСоСклада)AS date) AS datetime) AS МинимальнаяДата
	From 
		#Temp_ShipmentDatesPickUp
    )
SELECT
	DATEADD(dd,t.N-1,f.МинимальнаяДата) AS Date
INTO #Temp_Dates
FROM Temp_PickupDatesGroup f
  CROSS APPLY (SELECT TOP (Isnull(DATEDIFF(dd,f.МинимальнаяДата,f.МаксимальнаяДата)+1,1))
        N
    FROM cteTally
    ORDER BY N) t
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select 
	DATEADD(
		SECOND,
		CAST(
			DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27057, ПВЗГрафикРаботы._Fld23617)) AS NUMERIC(12)
		),
		date) AS ВремяНачала,
	DATEADD(
		SECOND,
		CAST(
			DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27058, ПВЗГрафикРаботы._Fld23618)) AS NUMERIC(12)
		),
		date) AS ВремяОкончания,
	Склады._IDRRef AS СкладНазначения--,
INTO #Temp_PickupWorkingHours
From 
	#Temp_Dates
	Inner Join dbo._Reference226 Склады WITH(NOLOCK)
		ON Склады._IDRRef IN (Select СкладСсылка From #Temp_PickupPoints)
	Inner Join _Reference23612 WITH(NOLOCK)
		On Склады._Fld23620RRef = _Reference23612._IDRRef
	Left Join _Reference23612_VT23613 As ПВЗГрафикРаботы WITH(NOLOCK)
		On _Reference23612._IDRRef = _Reference23612_IDRRef
			AND (case when @@DATEFIRST = 1 then DATEPART ( dw , #Temp_Dates.date ) when DATEPART ( dw , #Temp_Dates.date ) = 1 then 7 else DATEPART ( dw , #Temp_Dates.date ) -1 END) = ПВЗГрафикРаботы._Fld23615
	Left Join _Reference23612_VT27054 As ПВЗИзмененияГрафикаРаботы WITH(NOLOCK)
		On _Reference23612._IDRRef = ПВЗИзмененияГрафикаРаботы._Reference23612_IDRRef
			AND #Temp_Dates.date = ПВЗИзмененияГрафикаРаботы._Fld27056
Where
	case 
		when ПВЗИзмененияГрафикаРаботы._Reference23612_IDRRef is not null
			then ПВЗИзмененияГрафикаРаботы._Fld27059
		when ПВЗГрафикРаботы._Reference23612_IDRRef is not Null 
			then ПВЗГрафикРаботы._Fld25265 
		else 0 --не найдено ни графика ни изменения графика  
	end = 0x00  -- не выходной
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

SELECT
	#Temp_ShipmentDatesPickUp.НоменклатураСсылка,
	#Temp_ShipmentDatesPickUp.article,
	#Temp_ShipmentDatesPickUp.code,
	Min(CASE 
	WHEN 
		#Temp_PickupWorkingHours.ВремяНачала < #Temp_ShipmentDatesPickUp.ДатаСоСклада 
		then #Temp_ShipmentDatesPickUp.ДатаСоСклада
	Else
		#Temp_PickupWorkingHours.ВремяНачала
	End) As ВремяНачала
Into #Temp_AvailablePickUp
FROM
    #Temp_ShipmentDatesPickUp
		Inner {6} JOIN #Temp_PickupWorkingHours
		On #Temp_PickupWorkingHours.СкладНазначения = #Temp_ShipmentDatesPickUp.СкладНазначения
        And #Temp_PickupWorkingHours.ВремяОкончания > #Temp_ShipmentDatesPickUp.ДатаСоСклада
Group by
	#Temp_ShipmentDatesPickUp.НоменклатураСсылка,
	#Temp_ShipmentDatesPickUp.article,
	#Temp_ShipmentDatesPickUp.code
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);";

        public const string AvailableDate6IntervalsBasic = @"With PlanningGroups AS(
Select Distinct 
	#Temp_ShipmentDatesDeliveryCourier.ГруппаПланирования,
    #Temp_ShipmentDatesDeliveryCourier.Приоритет
From #Temp_ShipmentDatesDeliveryCourier
)
SELECT
    T5._Period AS Период,
    T5._Fld25112RRef As ГруппаПланирования, 
	T5._Fld25111RRef As Геозона,
	T5._Fld25202 As ВремяНачалаНачальное,
	T5._Fld25203 As ВремяОкончанияНачальное,
    DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T5._Fld25202) AS NUMERIC(12)
        ),
        T5._Period
    ) As ВремяНачала,
    PlanningGroups.Приоритет
into #Temp_IntervalsAll
FROM
    dbo._AccumRg25110 T5 With (NOLOCK)
    Inner Join PlanningGroups ON PlanningGroups.ГруппаПланирования = T5._Fld25112RRef
WHERE
    T5._Period BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd --begin +2
    AND T5._Fld25111RRef in (Select Геозона From #Temp_GeoData) 
GROUP BY
    T5._Period,
    T5._Fld25112RRef,
    T5._Fld25111RRef,
    T5._Fld25202,
	T5._Fld25203,
    PlanningGroups.Приоритет
HAVING
    (
        CAST(
            SUM(
                CASE
                    WHEN (T5._RecordKind = 0.0) THEN T5._Fld25113
                    ELSE -(T5._Fld25113)
                END
            ) AS NUMERIC(16, 0)
        ) > 0.0
    )
OPTION (HASH GROUP, OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}',@P_DateTimePeriodEnd='{3}'),KEEP PLAN, KEEPFIXED PLAN);";

        public const string AvailableDate6IntervalsCustom = @"With PlanningGroups AS(
Select Distinct 
	#Temp_ShipmentDatesDeliveryCourier.ГруппаПланирования,
	#Temp_ShipmentDatesDeliveryCourier.Приоритет
From #Temp_ShipmentDatesDeliveryCourier
)
SELECT
	T5.Период AS Период,
	T5.ГруппаПланирования As ГруппаПланирования, 
	T5.Геозона As Геозона,
	T5.ВремяНачала As ВремяНачалаНачальное,
	T5.ВремяОкончания As ВремяОкончанияНачальное,
	DATEADD(
		SECOND,
		CAST(
			DATEDIFF(SECOND, @P_EmptyDate, T5.ВремяНачала) AS NUMERIC(12)
		),
		T5.Период
	) As ВремяНачала,
    PlanningGroups.Приоритет
into #Temp_IntervalsAll
FROM
	[dbo].[IntervalsAggregate] T5 With (NOLOCK)
	Inner Join PlanningGroups ON PlanningGroups.ГруппаПланирования = T5.ГруппаПланирования
WHERE
	T5.Период BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd --begin +2
	AND T5.Геозона in (Select Геозона From #Temp_GeoData) 
	AND T5.КоличествоЗаказовЗаИнтервалВремени > 0
GROUP BY
	T5.Период,
	T5.ГруппаПланирования,
	T5.Геозона,
	T5.ВремяНачала,
	T5.ВремяОкончания,
    PlanningGroups.Приоритет
OPTION (HASH GROUP, OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}',@P_DateTimePeriodEnd='{3}'),KEEP PLAN, KEEPFIXED PLAN);";

        public const string AvailableDate7 = @"
select
DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
    #Temp_IntervalsAll.Период,
    #Temp_IntervalsAll.ГруппаПланирования,
    #Temp_IntervalsAll.Геозона,
    #Temp_IntervalsAll.Приоритет
into #Temp_Intervals
from #Temp_IntervalsAll
	Inner Join _Reference114_VT25126 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And #Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld25128
		And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld25129
   INNER JOIN dbo._Reference23294 T2 With (NOLOCK) 
		ON (#Temp_IntervalsAll.ГруппаПланирования = T2._IDRRef)
		AND (ГеоЗонаВременныеИнтервалы._Fld25128 >= T2._Fld25137)
		AND (NOT (((@P_TimeNow >= T2._Fld25138))))
WHERE
    #Temp_IntervalsAll.Период = @P_DateTimePeriodBegin
Group By 
	ГеоЗонаВременныеИнтервалы._Fld25128,
	ГеоЗонаВременныеИнтервалы._Fld25129,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	T2._Fld25137,
    #Temp_IntervalsAll.Приоритет
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}'), KEEP PLAN, KEEPFIXED PLAN);

INsert into #Temp_Intervals
select
DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
    #Temp_IntervalsAll.Период,
    #Temp_IntervalsAll.ГруппаПланирования,
    #Temp_IntervalsAll.Геозона,
    #Temp_IntervalsAll.Приоритет
from #Temp_IntervalsAll
	Inner Join _Reference114_VT25126 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And #Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld25128
		And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld25129
  INNER JOIN dbo._Reference23294 T4 With (NOLOCK) ON (#Temp_IntervalsAll.ГруппаПланирования = T4._IDRRef)
    AND (
        (@P_TimeNow < T4._Fld25140)
        OR (ГеоЗонаВременныеИнтервалы._Fld25128 >= T4._Fld25139)
    )
WHERE
    #Temp_IntervalsAll.Период = DATEADD(DAY, 1, @P_DateTimePeriodBegin)
Group By 
	ГеоЗонаВременныеИнтервалы._Fld25128,
	ГеоЗонаВременныеИнтервалы._Fld25129,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
    #Temp_IntervalsAll.Приоритет
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}'), KEEP PLAN, KEEPFIXED PLAN);

INsert into #Temp_Intervals
select distinct
DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
    #Temp_IntervalsAll.Период,
    #Temp_IntervalsAll.ГруппаПланирования,
    #Temp_IntervalsAll.Геозона,
    #Temp_IntervalsAll.Приоритет
from #Temp_IntervalsAll
	Inner Join _Reference114_VT25126 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And #Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld25128
		And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld25129
WHERE
	#Temp_IntervalsAll.Период BETWEEN DATEADD(DAY, 2, @P_DateTimePeriodBegin) AND @P_DateTimePeriodEnd --begin +2
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}',@P_DateTimePeriodEnd='{3}'), KEEP PLAN, KEEPFIXED PLAN);

Select Distinct ГруппаПланирования, ОсновнаяГруппаПланирования, Приоритет
Into #Temp_PlanningGroups
From #Temp_Goods t1
Where СкладСсылка is null
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select Distinct 
	t1.ГруппаПланирования, 
	Case When t1.Приоритет = 0 And t2.Период is null
		Then 1
		Else t1.Приоритет
	End As Приоритет
Into #Temp_PlanningGroupsPriority
From #Temp_PlanningGroups t1
	Left Join #Temp_Intervals t2
	On t1.ОсновнаяГруппаПланирования = t2.ГруппаПланирования
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select ВремяНачала, Период, t1.ГруппаПланирования, Геозона, t2.Приоритет
Into #Temp_IntervalsWithGroupPriority
From #Temp_Intervals t1
	Inner Join #Temp_PlanningGroupsPriority t2
	On t1.ГруппаПланирования = t2.ГруппаПланирования
OPTION (KEEP PLAN, KEEPFIXED PLAN);
";

        public const string AvailableDate8DeliveryPowerBasic = @"With Temp_DeliveryPower AS
(
SELECT
    SUM(
        CASE
            WHEN (МощностиДоставки._RecordKind = 0.0) THEN МощностиДоставки._Fld25107
            ELSE -(МощностиДоставки._Fld25107)
        END        
    ) AS МассаОборот,    
    SUM(
        CASE
            WHEN (МощностиДоставки._RecordKind = 0.0) THEN МощностиДоставки._Fld25108
            ELSE -(МощностиДоставки._Fld25108)
        END        
    ) AS ОбъемОборот,    
    SUM(
        CASE
            WHEN (МощностиДоставки._RecordKind = 0.0) THEN МощностиДоставки._Fld25201
            ELSE -(МощностиДоставки._Fld25201)
        END        
    ) AS ВремяНаОбслуживаниеОборот,
    CAST(CAST(МощностиДоставки._Period  AS DATE) AS DATETIME) AS Дата
FROM
    dbo._AccumRg25104 МощностиДоставки With (NOLOCK)
WHERE
    МощностиДоставки._Period BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd
    AND МощностиДоставки._Fld25105RRef IN (Select ЗонаДоставкиРодительСсылка From  #Temp_GeoData)
GROUP BY
    CAST(CAST(МощностиДоставки._Period  AS DATE) AS DATETIME)
), ";

        public const string AvailableDate8DeliveryPowerCustom = @"With Temp_DeliveryPower AS
(
SELECT   
    МощностиДоставки.МассаОборот AS МассаОборот,    
    МощностиДоставки.ОбъемОборот AS ОбъемОборот,    
    МощностиДоставки.ВремяНаОбслуживаниеОборот AS ВремяНаОбслуживаниеОборот,
    МощностиДоставки.Период AS Дата
FROM
    [dbo].[DeliveryPowerAggregate] МощностиДоставки With (NOLOCK)
WHERE
    МощностиДоставки.Период BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd
	AND МощностиДоставки.ЗонаДоставки IN (Select ЗонаДоставкиРодительСсылка From  #Temp_GeoData)
),";

        public const string AvailableDate9 = @"Temp_PlanningGroupPriority AS
(
    Select Период, Max(Приоритет) As Приоритет From #Temp_IntervalsWithGroupPriority Group By Период
)
SELECT
    T1.НоменклатураСсылка,
    T1.article,
    T1.code,
    MIN(
        ISNULL(
            T3.ВремяНачала,
CASE
                WHEN (T1.ДатаСоСклада > DATEADD(SECOND,-1,@P_DateTimePeriodEnd)) THEN DATEADD(
                    DAY,
                    1.0,
                    CAST(CAST(T1.ДатаСоСклада AS DATE) AS DATETIME)
                )
                ELSE DATEADD(DAY,1,@P_DateTimePeriodEnd)
            END
        )
    ) AS ДатаКурьерскойДоставки
Into #Temp_AvailableCourier
FROM
    #Temp_ShipmentDatesDeliveryCourier T1 WITH(NOLOCK)
    Left JOIN Temp_DeliveryPower T2 --WITH(NOLOCK)
        Inner JOIN #Temp_IntervalsWithGroupPriority T3 WITH(NOLOCK)
            Inner Join Temp_PlanningGroupPriority With (NOLOCK) 
            ON T3.Период = Temp_PlanningGroupPriority.Период 
            AND T3.Приоритет = Temp_PlanningGroupPriority.Приоритет
		ON T3.Период = T2.Дата
	ON T2.МассаОборот >= T1.Вес
    AND T2.ОбъемОборот >= T1.Объем
    AND T2.ВремяНаОбслуживаниеОборот >= T1.ВремяНаОбслуживание
    AND T2.Дата >= 
		CAST(CAST(T1.ДатаСоСклада AS DATE) AS DATETIME)    
    AND T3.ГруппаПланирования = T1.ГруппаПланирования
    AND T3.ВремяНачала >= T1.ДатаСоСклада
	AND T1.PickUp = 0
GROUP BY
	T1.НоменклатураСсылка,
    T1.article,
	T1.code
OPTION (HASH GROUP, OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}',@P_DateTimePeriodEnd='{3}'),KEEP PLAN, KEEPFIXED PLAN);

Select Top 1 
	DATEDIFF(Minute, ИнтервалыДоставкиВВашеВремя._Fld30390, ИнтервалыДоставкиВВашеВремя._Fld30391) As YourTimeInterval
Into #Temp_YourTimeInterval
From 
	#Temp_GeoData Геозоны
	Inner Join _Reference114_VT30388 ИнтервалыДоставкиВВашеВремя WITH(NOLOCK)
		On ИнтервалыДоставкиВВашеВремя._Reference114_IDRRef = Геозоны.Геозона
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

Select 
	IsNull(#Temp_AvailableCourier.article,#Temp_AvailablePickUp.article) AS Article,
	IsNull(#Temp_AvailableCourier.code,#Temp_AvailablePickUp.code) AS Code,
	IsNull(#Temp_AvailableCourier.ДатаКурьерскойДоставки,@P_MaxDate) AS Courier,
	IsNull(#Temp_AvailablePickUp.ВремяНачала,@P_MaxDate) AS Self,
	IsNull(#Temp_YourTimeInterval.YourTimeInterval, 0) YourTimeInterval
From
	#Temp_AvailableCourier 
	FULL Join #Temp_AvailablePickUp 
		On #Temp_AvailableCourier.НоменклатураСсылка = #Temp_AvailablePickUp.НоменклатураСсылка
	LEFT OUTER JOIN #Temp_YourTimeInterval
		On 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN)";

        public const string AvailableDateWithCount1 = @"
WITH cte AS (
    SELECT distinct value AS PickupPoint
    FROM #Temp_GoodsRaw t1
    CROSS APPLY (
        SELECT value
        FROM STRING_SPLIT(t1.PickupPoint, ',')
		WHERE t1.PickupPoint is not null
    ) t2
)
Select 
	Склады._IDRRef AS СкладСсылка,
	Склады._Fld19544 AS ERPКодСклада
Into #Temp_PickupPoints
From 
	dbo._Reference226 Склады WITH(NOLOCK)
Where Склады._Fld19544 in (select PickupPoint from cte)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select
	IsNull(_Reference114_VT23370._Fld23372RRef,Геозона._Fld23104RRef) As СкладСсылка,
	ЗоныДоставки._ParentIDRRef As ЗонаДоставкиРодительСсылка,
	Геозона._IDRRef As Геозона
Into #Temp_GeoData
From dbo._Reference114 Геозона With (NOLOCK)
	Inner Join _Reference114_VT23370 With (NOLOCK)
	on _Reference114_VT23370._Reference114_IDRRef = Геозона._IDRRef
	Inner Join _Reference99 ЗоныДоставки With (NOLOCK)
	on Геозона._Fld2847RRef = ЗоныДоставки._IDRRef
where Геозона._IDRRef IN (
	SELECT TOP 1
		T3._Fld26708RRef AS Fld26823RRef --геозона из рс векРасстоянияАВ
	FROM (SELECT
			T1._Fld25549 AS Fld25549_,
			MAX(T1._Period) AS MAXPERIOD_ 
		FROM dbo._InfoRg21711 T1 With (NOLOCK)
		WHERE T1._Fld26708RRef <> 0x00 and T1._Fld25549 = @P_CityCode
		GROUP BY T1._Fld25549) T2
	INNER JOIN dbo._InfoRg21711 T3 With (NOLOCK)
	ON T2.Fld25549_ = T3._Fld25549 AND T2.MAXPERIOD_ = T3._Period
	)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select Distinct
	Номенклатура._IDRRef AS НоменклатураСсылка,
	Номенклатура._Code AS code,
	Номенклатура._Fld3480 AS article,
	Номенклатура._Fld3489RRef AS ЕдиницаИзмерения,
	Номенклатура._Fld3526RRef AS Габариты,
	Упаковки._IDRRef AS УпаковкаСсылка,
	Упаковки._Fld6000 AS Вес,
	Упаковки._Fld6006 AS Объем,
	Упаковки._Fld6001 AS Высота,
	Упаковки._Fld6002 AS Глубина,
	Упаковки._Fld6009 AS Ширина,
    Номенклатура._Fld21822RRef as ТНВЭДСсылка,
    Номенклатура._Fld3515RRef as ТоварнаяКатегорияСсылка
INTO #Temp_GoodsPackages
From
	#Temp_GoodsRaw T1
	Inner Join 	dbo._Reference149 Номенклатура With (NOLOCK) 
		ON T1.code is NULL 
		and T1.PickupPoint is null
		and T1.Article = Номенклатура._Fld3480
	Inner Join dbo._Reference256 Упаковки With (NOLOCK)
		On 
		Упаковки._OwnerID_TYPE = 0x08  
		AND Упаковки.[_OwnerID_RTRef] = 0x00000095
		AND 
		Номенклатура._IDRRef = Упаковки._OwnerID_RRRef		
		And Упаковки._Fld6003RRef = Номенклатура._Fld3489RRef
		AND Упаковки._Marked = 0x00
union all
Select Distinct
	Номенклатура._IDRRef,
	Номенклатура._Code,
	Номенклатура._Fld3480,
	Номенклатура._Fld3489RRef,
	Номенклатура._Fld3526RRef,
	Упаковки._IDRRef AS УпаковкаСсылка,
	Упаковки._Fld6000 AS Вес,
	Упаковки._Fld6006 AS Объем,
	Упаковки._Fld6001 AS Высота,
	Упаковки._Fld6002 AS Глубина,
	Упаковки._Fld6009 AS Ширина,
    Номенклатура._Fld21822RRef as ТНВЭДСсылка,
    Номенклатура._Fld3515RRef as ТоварнаяКатегорияСсылка
From 
	#Temp_GoodsRaw T1
	Inner Join 	dbo._Reference149 Номенклатура With (NOLOCK) 
		ON T1.code is not NULL 
		and T1.PickupPoint is null
		and T1.code = Номенклатура._Code
	Inner Join dbo._Reference256 Упаковки With (NOLOCK)
		On 
		Упаковки._OwnerID_TYPE = 0x08  
		AND Упаковки.[_OwnerID_RTRef] = 0x00000095
		AND 
		Номенклатура._IDRRef = Упаковки._OwnerID_RRRef		
		And Упаковки._Fld6003RRef = Номенклатура._Fld3489RRef
		AND Упаковки._Marked = 0x00
OPTION (KEEP PLAN, KEEPFIXED PLAN);

WITH cte AS (
    SELECT distinct value AS PickupPoint
    FROM #Temp_GoodsRaw t1
    CROSS APPLY (
        SELECT value
        FROM STRING_SPLIT(t1.PickupPoint, ',')
		WHERE t1.PickupPoint is not null
    ) t2
)
SELECT Distinct
    t1.Article, t1.code, t1.quantity, cte.PickupPoint
INTO #Temp_GoodsRawParsed
FROM #Temp_GoodsRaw t1
Left JOIN cte
 ON t1.PickupPoint is not null
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select 
	Номенклатура.НоменклатураСсылка AS НоменклатураСсылка,
	Номенклатура.code AS code,
	Номенклатура.article AS article,
	Номенклатура.ЕдиницаИзмерения AS ЕдиницаИзмерения,
	Номенклатура.Габариты AS Габариты,
    T1.quantity AS Количество,
	#Temp_PickupPoints.СкладСсылка AS СкладПВЗСсылка,
	Номенклатура.УпаковкаСсылка AS УпаковкаСсылка,
	Номенклатура.Вес AS Вес,
	Номенклатура.Объем AS Объем,
	Номенклатура.Высота AS Высота,
	Номенклатура.Глубина AS Глубина,
	Номенклатура.Ширина AS Ширина,
    Номенклатура.ТНВЭДСсылка as ТНВЭДСсылка,
    Номенклатура.ТоварнаяКатегорияСсылка as ТоварнаяКатегорияСсылка
INTO #Temp_GoodsBegin
From
	#Temp_GoodsRawParsed T1
	Inner Join #Temp_GoodsPackages Номенклатура With (NOLOCK) 
		ON T1.code is NULL and T1.Article = Номенклатура.article
	Left Join #Temp_PickupPoints  
		ON T1.PickupPoint = #Temp_PickupPoints.ERPКодСклада

union all

Select 
	Номенклатура.НоменклатураСсылка AS НоменклатураСсылка,
	Номенклатура.code AS code,
	Номенклатура.article AS article,
	Номенклатура.ЕдиницаИзмерения AS ЕдиницаИзмерения,
	Номенклатура.Габариты AS Габариты,
    T1.quantity AS Количество,
	#Temp_PickupPoints.СкладСсылка,
	Номенклатура.УпаковкаСсылка AS УпаковкаСсылка,
	Номенклатура.Вес AS Вес,
	Номенклатура.Объем AS Объем,
	Номенклатура.Высота AS Высота,
	Номенклатура.Глубина AS Глубина,
	Номенклатура.Ширина AS Ширина,
    Номенклатура.ТНВЭДСсылка as ТНВЭДСсылка,
    Номенклатура.ТоварнаяКатегорияСсылка as ТоварнаяКатегорияСсылка
From 
	#Temp_GoodsRawParsed T1
	Inner Join #Temp_GoodsPackages Номенклатура With (NOLOCK) 
		ON T1.code is not NULL and T1.code = Номенклатура.code
	Left Join #Temp_PickupPoints  
		ON T1.PickupPoint = #Temp_PickupPoints.ERPКодСклада
OPTION (KEEP PLAN, KEEPFIXED PLAN);


/*Размеры для расчета габаритов*/
SELECT
T1.НоменклатураСсылка,
CAST(SUM((T1.Вес * T1.Количество)) AS NUMERIC(36, 6)) AS Вес,
CAST(SUM((T1.Объем * T1.Количество)) AS NUMERIC(38, 8)) AS Объем,
MAX(T1.Высота) AS Высота,
MAX(T1.Глубина) AS Глубина,
MAX(T1.Ширина) AS Ширина,
0x00000000000000000000000000000000  AS Габарит
Into #Temp_Size
FROM #Temp_GoodsBegin T1 WITH(NOLOCK)
Group By 
	T1.НоменклатураСсылка
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Габарит общий*/
SELECT
    --TOP 1 
	T1.НоменклатураСсылка,
	CASE
        WHEN (
            ISNULL(
                T1.Габарит,
                0x00000000000000000000000000000000
            ) <> 0x00000000000000000000000000000000
        ) THEN T1.Габарит
        WHEN (T4._Fld21339 > 0)
        AND (T1.Вес >= T4._Fld21339)
        AND (T5._Fld21337 > 0)
        AND (T1.Объем >= T5._Fld21337) THEN 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D --хбт в кбт
        WHEN (T2._Fld21168 > 0)
        AND (T1.Вес >= T2._Fld21168) THEN 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D --кбт
        WHEN (T3._Fld21166 > 0)
        AND (T1.Объем >= T3._Fld21166) THEN 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D --кбт
        WHEN (T6._Fld21580 > 0)
        AND (T1.Высота > 0)
        AND (T1.Глубина > 0)
        AND (T1.Ширина >0) THEN CASE
            WHEN (T1.Высота >= T6._Fld21580) OR (T1.Глубина >= T6._Fld21580) OR (T1.Ширина >= T6._Fld21580) THEN 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D --кбт
            ELSE 0x8AB421D483ABE88A4C4C9928262FFB0D --мбт
        END
        ELSE 0x8AB421D483ABE88A4C4C9928262FFB0D --мбт
    END AS Габарит
Into #Temp_Dimensions
FROM
    #Temp_Size T1 WITH(NOLOCK)
    INNER JOIN dbo._Const21167 T2 WITH(NOLOCK) ON 1 = 1
    INNER JOIN dbo._Const21165 T3 WITH(NOLOCK) ON 1 = 1
    INNER JOIN dbo._Const21338 T4 WITH(NOLOCK) ON 1 = 1
    INNER JOIN dbo._Const21336 T5 WITH(NOLOCK) ON 1 = 1
    INNER JOIN dbo._Const21579 T6 WITH(NOLOCK) ON 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select 
	Номенклатура.НоменклатураСсылка AS НоменклатураСсылка,
	Номенклатура.article AS article,
	Номенклатура.code AS code,
	Номенклатура.СкладПВЗСсылка AS СкладСсылка,
	Номенклатура.УпаковкаСсылка AS УпаковкаСсылка,--Упаковки._IDRRef AS УпаковкаСсылка,
	Номенклатура.Количество As Количество,
	Номенклатура.Вес AS Вес,--Упаковки._Fld6000 AS Вес,
	Номенклатура.Объем AS Объем,--Упаковки._Fld6006 AS Объем,
    Номенклатура.ТНВЭДСсылка as ТНВЭДСсылка,
    Номенклатура.ТоварнаяКатегорияСсылка as ТоварнаяКатегорияСсылка,
	10 AS ВремяНаОбслуживание,
	IsNull(ГруппыПланирования._IDRRef, 0x00000000000000000000000000000000) AS ГруппаПланирования,
	IsNull(ГруппыПланирования._IDRRef, 0x00000000000000000000000000000000) AS ОсновнаяГруппаПланирования,
	IsNull(ГруппыПланирования._Description, '') AS ГруппаПланированияНаименование,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(ГруппыПланирования._Fld25519, @P_EmptyDate)) AS ВремяНаПодготовкуПоставщик,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(ГруппыПланирования._Fld25520, @P_EmptyDate)) AS ВремяНаПодготовкуПеремещение,
	IsNull(ГруппыПланирования._Fld23302RRef, 0x00000000000000000000000000000000) AS ГруппаПланированияСклад,
	1 AS Приоритет
INTO #Temp_Goods
From 
	#Temp_GoodsBegin Номенклатура
    Inner Join #Temp_Dimensions With (NOLOCK) On Номенклатура.НоменклатураСсылка = #Temp_Dimensions.НоменклатураСсылка
	Left Join dbo._Reference23294 ГруппыПланирования With (NOLOCK)
		Inner Join dbo._Reference23294_VT23309 With (NOLOCK)
			on ГруппыПланирования._IDRRef = _Reference23294_VT23309._Reference23294_IDRRef
			and _Reference23294_VT23309._Fld23311RRef in (Select ЗонаДоставкиРодительСсылка From #Temp_GeoData)
		On 
		ГруппыПланирования._Fld23302RRef IN (Select СкладСсылка From #Temp_GeoData) --склад
		AND ГруппыПланирования._Fld25141 = 0x01--участвует в расчете мощности
		AND ГруппыПланирования._Fld23301RRef = #Temp_Dimensions.Габарит --Номенклатура.Габариты OR (Номенклатура.Габариты = 0xAC2CBF86E693F63444670FFEB70264EE AND ГруппыПланирования._Fld23301RRef= 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D) ) --габариты
		AND ГруппыПланирования._Marked = 0x00
        AND Номенклатура.СкладПВЗСсылка Is Null
UNION ALL
Select 
	Номенклатура.НоменклатураСсылка AS НоменклатураСсылка,
	Номенклатура.article AS article,
	Номенклатура.code AS code,
	Номенклатура.СкладПВЗСсылка AS СкладСсылка,
	Номенклатура.УпаковкаСсылка AS УпаковкаСсылка,--Упаковки._IDRRef AS УпаковкаСсылка,
	Номенклатура.Количество As Количество,
	Номенклатура.Вес AS Вес,--Упаковки._Fld6000 AS Вес,
	Номенклатура.Объем AS Объем,--Упаковки._Fld6006 AS Объем,
    Номенклатура.ТНВЭДСсылка as ТНВЭДСсылка,
    Номенклатура.ТоварнаяКатегорияСсылка as ТоварнаяКатегорияСсылка,
	10 AS ВремяНаОбслуживание,
	IsNull(ПодчиненнаяГП._IDRRef, 0x00000000000000000000000000000000) AS ГруппаПланирования,
	IsNull(ГруппыПланирования._IDRRef, 0x00000000000000000000000000000000) AS ОсновнаяГруппаПланирования,
	IsNull(ПодчиненнаяГП._Description, '') AS ГруппаПланированияНаименование,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(ПодчиненнаяГП._Fld25519, @P_EmptyDate)) AS ВремяНаПодготовкуПоставщик,
	DATEDIFF(SECOND, @P_EmptyDate, IsNull(ПодчиненнаяГП._Fld25520, @P_EmptyDate)) AS ВремяНаПодготовкуПеремещение,
	IsNull(ПодчиненнаяГП._Fld23302RRef, 0x00000000000000000000000000000000) AS ГруппаПланированияСклад,
	0
From 
	#Temp_GoodsBegin Номенклатура
	Inner Join #Temp_Dimensions With (NOLOCK) On Номенклатура.НоменклатураСсылка = #Temp_Dimensions.НоменклатураСсылка
	Left Join dbo._Reference23294 ГруппыПланирования With (NOLOCK)
		Inner Join dbo._Reference23294_VT23309 With (NOLOCK)
			on ГруппыПланирования._IDRRef = _Reference23294_VT23309._Reference23294_IDRRef
			and _Reference23294_VT23309._Fld23311RRef in (Select ЗонаДоставкиРодительСсылка From #Temp_GeoData)
		On 
		ГруппыПланирования._Fld23302RRef IN (Select СкладСсылка From #Temp_GeoData) --склад
		AND ГруппыПланирования._Fld25141 = 0x01--участвует в расчете мощности
		AND ГруппыПланирования._Fld23301RRef = #Temp_Dimensions.Габарит--(ГруппыПланирования._Fld23301RRef = Номенклатура.Габариты OR (Номенклатура.Габариты = 0xAC2CBF86E693F63444670FFEB70264EE AND ГруппыПланирования._Fld23301RRef= 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D) ) --габариты
		AND ГруппыПланирования._Marked = 0x00
		AND Номенклатура.СкладПВЗСсылка Is Null
	Inner Join dbo._Reference23294 ПодчиненнаяГП WITH(NOLOCK)
			On  ГруппыПланирования._Fld26526RRef = ПодчиненнаяГП._IDRRef
Where 
	Номенклатура.СкладПВЗСсылка IS NULL
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select 
	ЦеныТолькоПрайсы._Fld21408RRef AS Номенклатура,
	ЦеныТолькоПрайсы._Fld21410_TYPE AS Регистратор_TYPE,
    ЦеныТолькоПрайсы._Fld21410_RTRef AS Регистратор_RTRef,
    ЦеныТолькоПрайсы._Fld21410_RRRef AS Регистратор_RRRef
Into #Temp_Megaprices
From _AccumRg21407 ЦеныТолькоПрайсы With (NOLOCK)
	Inner Join _InfoRgSL26678 ExchangeRates With (NOLOCK)
		ON ЦеныТолькоПрайсы._Fld21443RRef = ExchangeRates._Fld14558RRef
		AND ЦеныТолькоПрайсы._Fld21410_RTRef = 0x00000153 --Цены.Регистратор ССЫЛКА Документ.мегапрайсРегистрацияПрайса
		AND (ЦеныТолькоПрайсы._Fld21982 <> 0 
			AND ЦеныТолькоПрайсы._Fld21442 * ExchangeRates._Fld14559 / ExchangeRates._Fld14560 >= ЦеныТолькоПрайсы._Fld21982 OR ЦеныТолькоПрайсы._Fld21411 >= ЦеныТолькоПрайсы._Fld21616)
		And ЦеныТолькоПрайсы._Fld21408RRef IN(SELECT
                НоменклатураСсылка
            FROM
                #Temp_GoodsPackages)
OPTION (KEEP PLAN, KEEPFIXED PLAN);
				
Select Distinct
	Цены._Fld21408RRef AS Номенклатура,
	Цены._Fld21410_RRRef AS РегистраторЦен_RRRef,
	Цены._Fld21442 * ExchangeRates._Fld14559 / ExchangeRates._Fld14560 AS Цена
Into #Temp_Prices
From _AccumRg21407 Цены With (NOLOCK)
	Inner Join _InfoRgSL26678 ExchangeRates With (NOLOCK)
		ON Цены._Fld21443RRef = ExchangeRates._Fld14558RRef
		AND Цены._Fld21410_RTRef IN(0x00000141,0x00000153)  --Регистратор ССЫЛКА Документ.мегапрайсРегистрацияПрайса, ЗаказПоставщику
		AND (Цены._Fld21982 <> 0 
			AND Цены._Fld21410_RTRef = 0x00000141 OR (Цены._Fld21442 * ExchangeRates._Fld14559 / ExchangeRates._Fld14560 >= Цены._Fld21982 OR Цены._Fld21411 >= Цены._Fld21616))
		AND Цены._Fld21408RRef IN(SELECT
                НоменклатураСсылка
            FROM
                #Temp_GoodsPackages)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

WITH FilteredGoods AS (
    SELECT
        TNomen.НоменклатураСсылка
    FROM
        #Temp_GoodsPackages TNomen WITH(NOLOCK)
)
SELECT
    T2._Fld21408RRef AS НоменклатураСсылка,
    T2._Fld21410_TYPE AS Источник_TYPE,
    T2._Fld21410_RTRef AS Источник_RTRef,
    T2._Fld21410_RRRef AS Источник_RRRef,
    #Temp_Megaprices.Регистратор_TYPE AS Регистратор_TYPE,
    #Temp_Megaprices.Регистратор_RTRef AS Регистратор_RTRef,
    #Temp_Megaprices.Регистратор_RRRef AS Регистратор_RRRef,
    T2._Fld23568RRef AS СкладИсточника,
    T2._Fld21424 AS ДатаСобытия,
    #Temp_Prices.Цена AS Цена,
    SUM(T2._Fld21411) - SUM(T2._Fld21412) AS Количество
Into #Temp_Remains
FROM
    dbo._AccumRgT21444 T2 WITH (NOLOCK)
	INNER JOIN FilteredGoods FG 
	ON T2._Fld21408RRef = FG.НоменклатураСсылка
		AND T2._Period = '5999-11-01 00:00:00'
		AND (T2._Fld21424 = '2001-01-01 00:00:00'
			OR (T2._Fld21424 >= @P_DateTimeNow))
		AND (T2._Fld21412 <> 0 OR T2._Fld21411 <> 0)
	LEFT JOIN #Temp_Megaprices
	ON T2._Fld21408RRef = #Temp_Megaprices.Номенклатура
	AND T2._Fld21410_RRRef = #Temp_Megaprices.Регистратор_RRRef
    LEFT JOIN #Temp_Prices
	ON T2._Fld21408RRef = #Temp_Prices.Номенклатура
	AND T2._Fld21410_RRRef = #Temp_Prices.РегистраторЦен_RRRef
	
GROUP BY
    T2._Fld21408RRef,
    T2._Fld21410_TYPE,
    T2._Fld21410_RTRef,
    T2._Fld21410_RRRef,
    #Temp_Megaprices.Регистратор_TYPE,
	#Temp_Megaprices.Регистратор_RTRef,
	#Temp_Megaprices.Регистратор_RRRef,
    T2._Fld23568RRef,
    T2._Fld21424,
	#Temp_Prices.Цена
HAVING
    (SUM(T2._Fld21412) <> 0.0
    OR SUM(T2._Fld21411) <> 0.0)
    AND SUM(T2._Fld21411) - SUM(T2._Fld21412) > 0.0
OPTION (HASH GROUP, OPTIMIZE FOR (@P_DateTimeNow='{1}'),KEEP PLAN, KEEPFIXED PLAN);

SELECT Distinct
    T1._Fld23831RRef AS СкладИсточника,
    T1._Fld23832 AS ДатаСобытия,
    T1._Fld23834 AS ДатаПрибытия,
    T1._Fld23833RRef AS СкладНазначения
Into #Temp_WarehouseDates
FROM
    dbo._InfoRg23830 T1 With (NOLOCK)
	Inner Join #Temp_Remains With (NOLOCK)
	ON T1._Fld23831RRef = #Temp_Remains.СкладИсточника
	AND T1._Fld23832 = #Temp_Remains.ДатаСобытия
	AND T1._Fld23833RRef IN (Select СкладСсылка From #Temp_GeoData UNION ALL Select СкладСсылка From #Temp_PickupPoints)
OPTION (KEEP PLAN, KEEPFIXED PLAN);";

        public const string AvailableDateWithCount3 = @"SELECT
    T1.НоменклатураСсылка,
    T1.Количество,
    T1.Источник_TYPE,
    T1.Источник_RTRef,
    T1.Источник_RRRef,
    T1.СкладИсточника,
    T1.Цена,
    T1.ДатаСобытия,
    ISNULL(T3.ДатаПрибытия, T2.ДатаПрибытия) AS ДатаДоступности,
    1 AS ТипИсточника,
    1 AS ЭтоСклад,
	ISNULL(T3.СкладНазначения, T2.СкладНазначения) AS СкладНазначения
INTO #Temp_Sources
FROM
    #Temp_Remains T1 WITH(NOLOCK)
    LEFT OUTER JOIN #Temp_WarehouseDates T2 WITH(NOLOCK)
    ON (T1.СкладИсточника = T2.СкладИсточника)
    AND (T1.ДатаСобытия = T2.ДатаСобытия)
    LEFT OUTER JOIN #Temp_MinimumWarehouseDates T3 WITH(NOLOCK)
    ON (T1.СкладИсточника = T3.СкладИсточника)
    AND (T1.ДатаСобытия = '2001-01-01 00:00:00')
WHERE
    T1.Источник_RTRef = 0x000000E2 OR T1.Источник_RTRef = 0x00000150 --склад, корректировка регистров

UNION
ALL
SELECT
    T4.НоменклатураСсылка,
    T4.Количество,
    T4.Источник_TYPE,
    T4.Источник_RTRef,
    T4.Источник_RRRef,
    T4.СкладИсточника,
    T4.Цена,
    T4.ДатаСобытия,
    T5.ДатаПрибытия,
    2,
    1,
	T5.СкладНазначения
FROM
    #Temp_Remains T4 WITH(NOLOCK)
    INNER JOIN #Temp_WarehouseDates T5 WITH(NOLOCK)
    ON (T4.СкладИсточника = T5.СкладИсточника)
    AND (T4.ДатаСобытия = T5.ДатаСобытия)
WHERE
    T4.Источник_RTRef = 0x00000141 --Заказ поставщику

UNION
ALL
SELECT
    T6.НоменклатураСсылка,
    T6.Количество,
    T6.Источник_TYPE,
    T6.Источник_RTRef,
    T6.Источник_RRRef,
    T6.СкладИсточника,
    T6.Цена,
    T6.ДатаСобытия,
    T7.ДатаПрибытия,
    3,
    0,
	T7.СкладНазначения
FROM
    #Temp_Remains T6 WITH(NOLOCK)
    INNER JOIN #Temp_WarehouseDates T7 WITH(NOLOCK)
    ON (T6.СкладИсточника = T7.СкладИсточника)
    AND (T6.ДатаСобытия = T7.ДатаСобытия)
WHERE
    NOT T6.Регистратор_RRRef IS NULL
	And T6.Источник_RTRef = 0x00000153 --мегапрайсРегистрацияПрайса
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select
	T1.НоменклатураСсылка AS НоменклатураСсылка,
	Min(T1.ДатаДоступности) AS ДатаДоступности,
	T1.Источник_RRRef,
	T1.ЭтоСклад,
	T1.Количество AS Количество
Into #Temp_SourcesGrouped
From
	#Temp_Sources T1
Group By
	T1.НоменклатураСсылка,
	T1.ЭтоСклад,
	T1.Источник_RRRef,
	T1.Количество
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With TempSourcesGrouped AS
(
Select
	T1.НоменклатураСсылка AS НоменклатураСсылка,
	Min(Case When T1.ЭтоСклад = 1 Then T1.ДатаДоступности Else @P_MaxDate End) AS ДатаДоступностиСклад,
	Sum(Case When T1.ЭтоСклад = 1 Then T1.Количество Else 0 End) AS ОстатокНаСкладе,
	Sum(T1.Количество) AS ОстатокВсего
From
	#Temp_SourcesGrouped T1
Group By
	T1.НоменклатураСсылка
)
Select
	T1.НоменклатураСсылка,
	isNull(T2.ДатаДоступностиСклад, @P_MaxDate) AS ДатаДоступностиСклад,
	min(Case when T1.Количество <= isNull(T2.ОстатокВсего, 0) Then 1 Else 0 End) As ОстаткаДостаточно,
	min(Case when T1.Количество <= isNull(T2.ОстатокНаСкладе, 0) Then 1 Else 0 End) As ОстаткаНаСкладеДостаточно,
	min(Case when 0 < isNull(T2.ОстатокНаСкладе, 0) Then 1 Else 0 End) As ОстатокЕсть
Into #Temp_StockSourcesAvailable
From #Temp_Goods T1
	Left Join TempSourcesGrouped T2
	On T1.НоменклатураСсылка = T2.НоменклатураСсылка
Where @P_StockPriority = 1
Group By
    isNull(T2.ДатаДоступностиСклад, @P_MaxDate),
	T1.НоменклатураСсылка
Having 
    min(Case when 0 < isNull(T2.ОстатокНаСкладе, 0) Then 1 Else 0 End) = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select
	T1.НоменклатураСсылка AS НоменклатураСсылка,
	Sum(T1.Количество) AS Количество,
	T1.ЭтоСклад,
	T1.ДатаДоступности AS ДатаДоступности,
	T1.СкладНазначения AS СкладНазначения
Into #Temp_SourcesGrouped2
From
	#Temp_Sources T1	
Group by
	T1.НоменклатураСсылка,
	T1.ЭтоСклад,
	T1.ДатаДоступности,
	T1.СкладНазначения
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select
	Источники1.НоменклатураСсылка AS Номенклатура,
	Источники1.СкладНазначения AS СкладНазначения,
	Источники1.ДатаДоступности AS ДатаДоступности,
	Sum(Источник2.Количество) AS Количество
Into #Temp_AvailableGoods
From
	#Temp_SourcesGrouped2 AS Источники1
		Left Join #Temp_SourcesGrouped2 AS Источник2
		On Источники1.НоменклатураСсылка = Источник2.НоменклатураСсылка
		AND Источники1.СкладНазначения = Источник2.СкладНазначения
			AND Источники1.ДатаДоступности >= Источник2.ДатаДоступности
		Inner Join #Temp_StockSourcesAvailable
		on @P_StockPriority = 1
			AND Источники1.НоменклатураСсылка = #Temp_StockSourcesAvailable.НоменклатураСсылка
			AND ((Источники1.ЭтоСклад = 1
				AND #Temp_StockSourcesAvailable.ОстатокЕсть = 1
				AND #Temp_StockSourcesAvailable.ОстаткаДостаточно = 1)
			OR (Источники1.ЭтоСклад = 0
				AND #Temp_StockSourcesAvailable.ОстаткаДостаточно = 1
				AND #Temp_StockSourcesAvailable.ОстаткаНаСкладеДостаточно = 0
				AND Источники1.ДатаДоступности >= #Temp_StockSourcesAvailable.ДатаДоступностиСклад))
Group by
	Источники1.НоменклатураСсылка,
	Источники1.ДатаДоступности,
	Источники1.СкладНазначения

Union All

Select
	Источники1.НоменклатураСсылка AS Номенклатура,
	Источники1.СкладНазначения AS СкладНазначения,
	Источники1.ДатаДоступности AS ДатаДоступности,
	Sum(Источник2.Количество) AS Количество
From
	#Temp_SourcesGrouped2 AS Источники1
		Left Join #Temp_SourcesGrouped2 AS Источник2
		On Источники1.НоменклатураСсылка = Источник2.НоменклатураСсылка
		AND Источники1.СкладНазначения = Источник2.СкладНазначения
			AND Источники1.ДатаДоступности >= Источник2.ДатаДоступности	
		Left Join #Temp_StockSourcesAvailable
		On Источники1.НоменклатураСсылка = #Temp_StockSourcesAvailable.НоменклатураСсылка
Where @P_StockPriority = 0
    Or #Temp_StockSourcesAvailable.ОстатокЕсть is null
Group By
	Источники1.НоменклатураСсылка,
	Источники1.ДатаДоступности,
	Источники1.СкладНазначения
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);";

        public const string AvailableDateWithCount5 = @"
With Temp_SupplyDocs AS
(
SELECT
    T1.НоменклатураСсылка,
    T1.СкладНазначения,
    T1.ДатаДоступности,
    DATEADD(DAY, {4}, T1.ДатаДоступности) AS ДатаДоступностиПлюс, --это параметр КоличествоДнейАнализа
    MIN(T1.Цена) AS ЦенаИсточника,
    MIN(T1.Цена / 100.0 * (100 - {5})) AS ЦенаИсточникаМинус --это параметр ПроцентДнейАнализа
FROM
    #Temp_SourcesWithPrices T1 WITH(NOLOCK)
WHERE
    T1.Цена <> 0
    AND T1.Источник_RTRef = 0x00000153    
GROUP BY
    T1.НоменклатураСсылка,
    T1.ДатаДоступности,
    T1.СкладНазначения,
    DATEADD(DAY, {4}, T1.ДатаДоступности)
)
SELECT
    T2.НоменклатураСсылка,
    T2.ДатаДоступности,
    T2.СкладНазначения,
    T2.ДатаДоступностиПлюс,
    T2.ЦенаИсточника,
    T2.ЦенаИсточникаМинус,
    MIN(T1.ДатаДоступности) AS ДатаДоступности1,
    MIN(T1.Цена) AS Цена1
Into #Temp_BestPriceAfterClosestDate
FROM
    #Temp_SourcesWithPrices T1 WITH(NOLOCK)
    INNER HASH JOIN Temp_SupplyDocs T2 WITH(NOLOCK)
    ON (T1.НоменклатураСсылка = T2.НоменклатураСсылка)
    AND (T1.ДатаДоступности >= T2.ДатаДоступности)
    AND (T1.ДатаДоступности <= T2.ДатаДоступностиПлюс)
    AND (T1.Цена <= T2.ЦенаИсточникаМинус)
    AND (T1.Цена <> 0)
GROUP BY
    T2.НоменклатураСсылка,
    T2.СкладНазначения,
    T2.ДатаДоступностиПлюс,
    T2.ЦенаИсточника,
    T2.ЦенаИсточникаМинус,
    T2.ДатаДоступности
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);

With Temp_ClosestDate AS
(SELECT
T1.НоменклатураСсылка,
T1.СкладНазначения,
Cast(MIN(T1.ДатаДоступности)as datetime) AS ДатаДоступности
FROM #Temp_Sources T1 WITH(NOLOCK)
GROUP BY T1.НоменклатураСсылка,
T1.СкладНазначения
)
SELECT
            T4.НоменклатураСсылка,
            Min(T4.ДатаДоступности)AS ДатаДоступности,
            T4.СкладНазначения
		Into #Temp_T3
        FROM
            #Temp_Sources T4 WITH(NOLOCK)
            INNER JOIN Temp_ClosestDate T5 WITH(NOLOCK)
            ON (T4.НоменклатураСсылка = T5.НоменклатураСсылка)
            AND (T4.СкладНазначения = T5.СкладНазначения)
            AND (T4.ТипИсточника = 1)
			AND T4.ДатаДоступности <= DATEADD(DAY, {4}, T5.ДатаДоступности)
Group by T4.НоменклатураСсылка, T4.СкладНазначения
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
    #Temp_AvailableGoods.СкладНазначения AS СкладНазначения,
    #Temp_AvailableGoods.ДатаДоступности AS БлижайшаяДата,
    T1.Количество AS Количество,
    T1.Вес,
    T1.Объем,
    T1.ТНВЭДСсылка,
    T1.ТоварнаяКатегорияСсылка,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
	T1.ВремяНаПодготовкуПоставщик,
	T1.ВремяНаПодготовкуПеремещение,
	T1.Приоритет,
	0 AS PickUp
into #Temp_ClosestDatesByGoodsWithoutShiftingUngrupped
FROM
    #Temp_Goods T1 WITH(NOLOCK)	
   Left Join #Temp_AvailableGoods With (NOLOCK) 
			On T1.НоменклатураСсылка = #Temp_AvailableGoods.Номенклатура
			AND T1.Количество <= #Temp_AvailableGoods.Количество
			AND #Temp_AvailableGoods.СкладНазначения IN (Select СкладСсылка From #Temp_GeoData)
Where 
	T1.СкладСсылка IS NULL
	And T1.ГруппаПланированияСклад = #Temp_AvailableGoods.СкладНазначения 

UNION ALL
SELECT
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
    #Temp_AvailableGoods.СкладНазначения AS СкладНазначения,
    #Temp_AvailableGoods.ДатаДоступности AS БлижайшаяДата,
    T1.Количество AS Количество,
    T1.Вес,
    T1.Объем,
    T1.ТНВЭДСсылка,
    T1.ТоварнаяКатегорияСсылка,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
	T1.ВремяНаПодготовкуПоставщик,
	T1.ВремяНаПодготовкуПеремещение,
	T1.Приоритет,
	1 AS PickUp
FROM
	 #Temp_Goods T1 WITH(NOLOCK)	
	 Left Join #Temp_AvailableGoods With (NOLOCK) 
		On T1.НоменклатураСсылка = #Temp_AvailableGoods.Номенклатура
		AND T1.Количество <= #Temp_AvailableGoods.Количество
		AND	T1.СкладСсылка = #Temp_AvailableGoods.СкладНазначения
Where 
	T1.СкладСсылка IS NOT NULL
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
    T1.СкладНазначения AS СкладНазначения,
    Min(T1.БлижайшаяДата) AS БлижайшаяДата,
    T1.Количество AS Количество,
    T1.Вес,
    T1.Объем,
    T1.ТНВЭДСсылка,
    T1.ТоварнаяКатегорияСсылка,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
	T1.ВремяНаПодготовкуПоставщик,
	T1.ВремяНаПодготовкуПеремещение,
	T1.Приоритет,
	T1.PickUp
into #Temp_ClosestDatesByGoodsWithoutShifting
FROM
	 #Temp_ClosestDatesByGoodsWithoutShiftingUngrupped T1 WITH(NOLOCK)	
GROUP BY
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
	T1.СкладНазначения,
    T1.Вес,
    T1.Объем,
    T1.ТНВЭДСсылка,
    T1.ТоварнаяКатегорияСсылка,
    T1.ВремяНаОбслуживание,
    T1.Количество,
    T1.ГруппаПланирования,
	T1.ВремяНаПодготовкуПоставщик,
	T1.ВремяНаПодготовкуПеремещение,
	T1.Приоритет,
    T1.PickUp
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
    T1.СкладНазначения,
    Case when ПрослеживаемыеТоварныеКатегории._Fld28349RRef is null then T1.БлижайшаяДата else DateAdd(DAY, @P_DaysToShift, ПрослеживаемыеТоварныеКатегории._Period) end as БлижайшаяДата,
    T1.Количество,
    T1.Вес,
    T1.Объем,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
	T1.ВремяНаПодготовкуПоставщик,
	T1.ВремяНаПодготовкуПеремещение,
    T1.Приоритет,
	T1.PickUp
into #Temp_ClosestDatesByGoods
FROM
    #Temp_ClosestDatesByGoodsWithoutShifting T1 WITH(NOLOCK)
	left join dbo._InfoRg28348 as ПрослеживаемыеТоварныеКатегории WITH(NOLOCK)
		on 1 = @P_ApplyShifting 
			and ПрослеживаемыеТоварныеКатегории._Fld28349RRef = T1.ТоварнаяКатегорияСсылка 
			and T1.БлижайшаяДата BETWEEN ПрослеживаемыеТоварныеКатегории._Period AND DateAdd(DAY, @P_DaysToShift, ПрослеживаемыеТоварныеКатегории._Period)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T1.НоменклатураСсылка,
    T1.article,
    T1.code,
    T1.СкладНазначения,
    T1.Вес,
    T1.Объем,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
    MIN(
        CASE
            WHEN T1.СкладНазначения = T2.СкладИсточника
				AND (T2.Источник_RTRef = 0x00000141
					OR T2.Источник_RTRef = 0x00000153)
                THEN DATEADD(SECOND, T1.ВремяНаПодготовкуПоставщик, T1.БлижайшаяДата)
			WHEN T1.СкладНазначения <> T2.СкладИсточника
				THEN DATEADD(SECOND, T1.ВремяНаПодготовкуПеремещение, T1.БлижайшаяДата)
            ELSE T1.БлижайшаяДата
        END
    ) AS ДатаДоступности,
    T1.Приоритет,
	T1.PickUp
Into #Temp_ShipmentDates
FROM
    #Temp_ClosestDatesByGoods T1 WITH(NOLOCK)
    LEFT OUTER JOIN #Temp_Sources T2 WITH(NOLOCK)
    ON (T1.НоменклатураСсылка = T2.НоменклатураСсылка)
    AND (T1.СкладНазначения = T2.СкладНазначения)
    AND (T1.БлижайшаяДата = T2.ДатаДоступности)
Where 
	NOT T1.БлижайшаяДата IS NULL
GROUP BY
    T1.НоменклатураСсылка,
    T1.article,
    T1.code,
    T1.СкладНазначения,
    T1.Вес,
    T1.Объем,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
    T1.Приоритет,
	T1.PickUp
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With MinDates AS
(
Select 
	T1.НоменклатураСсылка,
    MIN(T1.ДатаДоступности) AS ДатаСоСклада
FROM 
    #Temp_ShipmentDates T1 WITH(NOLOCK)
Where T1.PickUp = 0
Group by T1.НоменклатураСсылка
)
SELECT
    T1.НоменклатураСсылка,
    T1.article,
    T1.code,
    MinDates.ДатаСоСклада AS ДатаСоСклада,
    T1.Вес,
    T1.Объем,
    T1.ВремяНаОбслуживание,
    T1.ГруппаПланирования,
    T1.Приоритет,
	T1.PickUp
Into #Temp_ShipmentDatesDeliveryCourier
FROM
    #Temp_ShipmentDates T1 WITH(NOLOCK)
    Inner Join MinDates
		On T1.НоменклатураСсылка = MinDates.НоменклатураСсылка 
		And T1.ДатаДоступности = MinDates.ДатаСоСклада 
Where T1.PickUp = 0
OPTION (KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
    MIN(DATEADD(HOUR, ISNULL(СмещениеДатДоставки.РазмерСмещения, 0), T1.ДатаДоступности)) AS ДатаСоСклада,
	T1.СкладНазначения
Into #Temp_ShipmentDatesPickUp
FROM 
    #Temp_ShipmentDates T1 WITH(NOLOCK)
	LEFT OUTER JOIN #Temp_PickupDateShift СмещениеДатДоставки
	On СмещениеДатДоставки.Склад = T1.СкладНазначения
	And T1.ДатаДоступности < DATEADD(DAY, 2, @P_DateTimePeriodBegin) -- дата меньше чем конец завтрашнего дня
Where T1.PickUp = 1
GROUP BY
    T1.НоменклатураСсылка,
	T1.article,
	T1.code,
	T1.СкладНазначения
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Это получение списка дат интервалов ПВЗ*/
WITH
    H1(N)
    AS
    (
        SELECT 1
        FROM (VALUES
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1),
                (1))H0(N)
    )
,
    cteTALLY(N)
    AS
    (
        SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
        FROM H1 a, H1 b, H1 c, H1 d, H1 e, H1 f, H1 g, H1 h
    ),
	Temp_PickupDatesGroup AS
	(
	Select 
		CAST(CAST(DateAdd(DAY, @P_DaysToShow,Max(ДатаСоСклада))AS date) AS datetime) AS МаксимальнаяДата,
		CAST(CAST(Min(ДатаСоСклада)AS date) AS datetime) AS МинимальнаяДата
	From 
		#Temp_ShipmentDatesPickUp
    )
SELECT
	DATEADD(dd,t.N-1,f.МинимальнаяДата) AS Date
INTO #Temp_Dates
FROM Temp_PickupDatesGroup f
  CROSS APPLY (SELECT TOP (Isnull(DATEDIFF(dd,f.МинимальнаяДата,f.МаксимальнаяДата)+1,1))
        N
    FROM cteTally
    ORDER BY N) t
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select 
	DATEADD(
		SECOND,
		CAST(
			DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27057, ПВЗГрафикРаботы._Fld23617)) AS NUMERIC(12)
		),
		date) AS ВремяНачала,
	DATEADD(
		SECOND,
		CAST(
			DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27058, ПВЗГрафикРаботы._Fld23618)) AS NUMERIC(12)
		),
		date) AS ВремяОкончания,
	Склады._IDRRef AS СкладНазначения--,
INTO #Temp_PickupWorkingHours
From 
	#Temp_Dates
	Inner Join dbo._Reference226 Склады WITH(NOLOCK)
		ON Склады._IDRRef IN (Select СкладСсылка From #Temp_PickupPoints)
	Inner Join _Reference23612 WITH(NOLOCK)
		On Склады._Fld23620RRef = _Reference23612._IDRRef
	Left Join _Reference23612_VT23613 As ПВЗГрафикРаботы WITH(NOLOCK)
		On _Reference23612._IDRRef = _Reference23612_IDRRef
			AND (case when @@DATEFIRST = 1 then DATEPART ( dw , #Temp_Dates.date ) when DATEPART ( dw , #Temp_Dates.date ) = 1 then 7 else DATEPART ( dw , #Temp_Dates.date ) -1 END) = ПВЗГрафикРаботы._Fld23615
	Left Join _Reference23612_VT27054 As ПВЗИзмененияГрафикаРаботы WITH(NOLOCK)
		On _Reference23612._IDRRef = ПВЗИзмененияГрафикаРаботы._Reference23612_IDRRef
			AND #Temp_Dates.date = ПВЗИзмененияГрафикаРаботы._Fld27056
Where
	case 
		when ПВЗИзмененияГрафикаРаботы._Reference23612_IDRRef is not null
			then ПВЗИзмененияГрафикаРаботы._Fld27059
		when ПВЗГрафикРаботы._Reference23612_IDRRef is not Null 
			then ПВЗГрафикРаботы._Fld25265 
		else 0 --не найдено ни графика ни изменения графика  
	end = 0x00  -- не выходной
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;	

SELECT
	#Temp_ShipmentDatesPickUp.НоменклатураСсылка,
	#Temp_ShipmentDatesPickUp.article,
	#Temp_ShipmentDatesPickUp.code,
	Min(CASE 
	WHEN 
		#Temp_PickupWorkingHours.ВремяНачала < #Temp_ShipmentDatesPickUp.ДатаСоСклада 
		then #Temp_ShipmentDatesPickUp.ДатаСоСклада
	Else
		#Temp_PickupWorkingHours.ВремяНачала
	End) As ВремяНачала
Into #Temp_AvailablePickUp
FROM
    #Temp_ShipmentDatesPickUp
		Inner {6} JOIN #Temp_PickupWorkingHours
		On #Temp_PickupWorkingHours.ВремяОкончания > #Temp_ShipmentDatesPickUp.ДатаСоСклада
		And #Temp_PickupWorkingHours.СкладНазначения = #Temp_ShipmentDatesPickUp.СкладНазначения
Group by
	#Temp_ShipmentDatesPickUp.НоменклатураСсылка,
	#Temp_ShipmentDatesPickUp.article,
	#Temp_ShipmentDatesPickUp.code
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);";
    }
}
