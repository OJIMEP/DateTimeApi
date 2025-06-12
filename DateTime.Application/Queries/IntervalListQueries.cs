namespace DateTimeService.Application.Queries
{
    public static class IntervalListQueries
    {
        public const string IntervalList = @"
Select 
	_IDRRef AS ЗаказСсылка,
	_Fld8243RRef AS ЗонаДоставки,
	_Fld8244 AS ВремяДоставкиС,
	_Fld8245 AS ВремяДоставкиПо,
	_Fld8205RRef AS ПВЗСсылка,
	_Fld8241RRef As СпособДоставки,
	_Fld8260RRef As АдресДоставки,
	_Fld21917RRef AS Габариты,
	Case When _Fld21650 = ''
		then @P_Floor
		Else
		Convert(numeric(2),_Fld21650)
	End As Этаж,
	_Fld25158 As Вес,
	_Fld25159 As Объем,
	_Date_Time,
	_Number
Into #Temp_OrderInfo
from dbo._Document317 OrderDocument
where 
	OrderDocument._Date_Time = @P_OrderDate 
	And OrderDocument._Number = @P_OrderNumber
	And _Fld8244 = '2001-01-01T01:00:00' 
	And _Fld8245 = '2001-01-01T23:00:00'

Select
	Товары._Fld8276RRef AS НоменклатураСсылка,
	_Fld8280 AS Количество,
    #Temp_OrderInfo.ПВЗСсылка AS Склад,
    #Temp_OrderInfo.ЗаказСсылка AS ЗаказСсылка
Into #Temp_GoodsOrder
From 
	dbo._Document317_VT8273 Товары
	Inner Join #Temp_OrderInfo
		On Товары._Document317_IDRRef = #Temp_OrderInfo.ЗаказСсылка

Select
	IsNull(_Reference114_VT23370._Fld23372RRef,Геозона._Fld23104RRef) As СкладСсылка,
	ЗоныДоставки._ParentIDRRef As ЗонаДоставкиРодительСсылка,
	ЗоныДоставкиРодитель._Description AS ЗонаДоставкиРодительНаименование,
    ЗоныДоставки._Fld31473 AS КоэффициентЗоныДоставки,
	Геозона._IDRRef As Геозона,
    Геозона._Fld33174 As УчетИспользованияМощностей,
    Геозона._Fld33171 As ЦелевойДень,
    Геозона._Fld33173 As ВремяСтопаНаЦелевойДень,
    Геозона._Fld33172 As ВремяНачалаДеньПослеЦелевого,
    Геозона._Fld32161 AS ПланируемыеИнтервалыС,
	Геозона._Fld32162 AS ПланируемыеИнтервалыПо
Into #Temp_GeoData
From dbo._Reference114 Геозона With (NOLOCK)
	Inner Join _Reference114_VT23370 With (NOLOCK)
	on _Reference114_VT23370._Reference114_IDRRef = Геозона._IDRRef
	Inner Join _Reference99 ЗоныДоставки With (NOLOCK)
	on Геозона._Fld2847RRef = ЗоныДоставки._IDRRef
	Inner Join _Reference99 ЗоныДоставкиРодитель With (NOLOCK)
	on ЗоныДоставки._ParentIDRRef = ЗоныДоставкиРодитель._IDRRef
where
	(@P_GeoCode = '' AND 
    @P_AdressCode <> '' And
Геозона._IDRRef IN (
	Select Top 1 --по адресу находим геозону
	ГеоАдрес._Fld2785RRef 
	From dbo._Reference112 ГеоАдрес With (NOLOCK)
	Where ГеоАдрес._Fld25155 = @P_AdressCode))
OR
(@P_GeoCode <> '' AND Геозона._Fld21249 = @P_GeoCode)
OR 
Геозона._Fld2847RRef In (select ЗонаДоставки from #Temp_OrderInfo Where #Temp_OrderInfo.СпособДоставки = 0x9B7EC3D470857E364E10EF7D3C09E30D) 
OPTION (KEEP PLAN, KEEPFIXED PLAN);
{0}
Select _IDRRef As СкладСсылка
Into #Temp_PickupPoints
From dbo._Reference226 Склады 
Where Склады._Fld19544 = @PickupPoint1
Union All
Select #Temp_OrderInfo.ПВЗСсылка from #Temp_OrderInfo
Where #Temp_OrderInfo.СпособДоставки = 0x9B5E4A5ABB206D854BE9B32BF442A653
OPTION (KEEP PLAN, KEEPFIXED PLAN);


/*Создание таблицы товаров и ее наполнение данными из БД*/
Select 
	Номенклатура._IDRRef AS НоменклатураСсылка,
	Упаковки._IDRRef AS УпаковкаСсылка,
    Номенклатура._Fld21822RRef as ТНВЭДСсылка,
	Номенклатура._Fld30392RRef As ДопКодТНВЭД,
    Номенклатура._Fld3515RRef as ТоварнаяКатегорияСсылка,
    0x00000000000000000000000000000000 AS Склад,
    Sum(T1.quantity) As Количество	
INTO #Temp_Goods
From 
	#Temp_GoodsRaw T1
	Inner Join 	dbo._Reference149 Номенклатура With (NOLOCK) 
		ON T1.code is NULL and T1.Article = Номенклатура._Fld3480
	Inner Join dbo._Reference256 Упаковки With (NOLOCK)
		On 
		Упаковки._OwnerID_TYPE = 0x08  
		AND Упаковки.[_OwnerID_RTRef] = 0x00000095
		AND Номенклатура._IDRRef = Упаковки._OwnerID_RRRef		
		And Упаковки._Fld6003RRef = Номенклатура._Fld3489RRef
		AND Упаковки._Marked = 0x00
Group By 
	Номенклатура._IDRRef,
	Упаковки._IDRRef,
    Номенклатура._Fld21822RRef,
	Номенклатура._Fld30392RRef,
    Номенклатура._Fld3515RRef
union all
Select 
	Номенклатура._IDRRef,
	Упаковки._IDRRef,
    Номенклатура._Fld21822RRef,
	Номенклатура._Fld30392RRef,
    Номенклатура._Fld3515RRef,
    0x00000000000000000000000000000000,
    Sum(T1.quantity)	
From 
	#Temp_GoodsRaw T1
	Inner Join 	dbo._Reference149 Номенклатура With (NOLOCK) 
		ON T1.code is not NULL and T1.code = Номенклатура._Code
	Inner Join dbo._Reference256 Упаковки With (NOLOCK)
		On 
		Упаковки._OwnerID_TYPE = 0x08  
		AND Упаковки.[_OwnerID_RTRef] = 0x00000095
		AND Номенклатура._IDRRef = Упаковки._OwnerID_RRRef		
		And Упаковки._Fld6003RRef = Номенклатура._Fld3489RRef
		AND Упаковки._Marked = 0x00
Group By 
	Номенклатура._IDRRef,
	Упаковки._IDRRef,
    Номенклатура._Fld21822RRef,
	Номенклатура._Fld30392RRef,
    Номенклатура._Fld3515RRef
union all
Select 
	Номенклатура._IDRRef,
	Упаковки._IDRRef,
    Номенклатура._Fld21822RRef,
	Номенклатура._Fld30392RRef,
    Номенклатура._Fld3515RRef,
    T1.Склад,
    Sum(T1.Количество)	
From 
	#Temp_GoodsOrder T1
	Inner Join 	dbo._Reference149 Номенклатура With (NOLOCK) 
		ON T1.НоменклатураСсылка = Номенклатура._IDRRef
	Inner Join dbo._Reference256 Упаковки With (NOLOCK)
		On 
		Упаковки._OwnerID_TYPE = 0x08  
		AND Упаковки.[_OwnerID_RTRef] = 0x00000095
		AND Номенклатура._IDRRef = Упаковки._OwnerID_RRRef		
		And Упаковки._Fld6003RRef = Номенклатура._Fld3489RRef
		AND Упаковки._Marked = 0x00
Where 
	Номенклатура._Fld3514RRef = 0x84A6131B6DC5555A4627E85757507687 -- тип номенклатуры товар
Group By 
	Номенклатура._IDRRef,
    T1.Склад,
    Упаковки._IDRRef,
    Номенклатура._Fld21822RRef,
	Номенклатура._Fld30392RRef,
    Номенклатура._Fld3515RRef
OPTION (KEEP PLAN, KEEPFIXED PLAN);
/*Конец товаров*/

/*Маркируемые коды ТНВЭД*/
SELECT DISTINCT
	T1.КодТНВЭД AS КодТНВЭД,
	T1.ДополнительныйКод
INTO #Temp_MarkedCodes
FROM (SELECT
	T4._Fld27184RRef AS КодТНВЭД,
	T4._Fld32086RRef As ДополнительныйКод
	FROM (SELECT
		T3._Fld27184RRef AS КодТНВЭД,
		T3._Fld32086RRef As ДополнительныйКод,
		MAX(T3._Period) AS MAXPERIOD_
		FROM dbo._InfoRg27183 T3
			INNER JOIN #Temp_Goods T5 WITH(NOLOCK)
			ON (T3._Fld27184RRef = T5.ТНВЭДСсылка)
		WHERE T3._Period <= @P_DateTimePeriodBegin
		GROUP BY T3._Fld27184RRef,
			T3._Fld32086RRef) T2
	INNER JOIN dbo._InfoRg27183 T4
	ON T2.КодТНВЭД = T4._Fld27184RRef 
	AND T2.ДополнительныйКод = T4._Fld32086RRef 
	AND T2.MAXPERIOD_ = T4._Period 
	AND T4._Fld28120 = 0x01) T1
;

WITH НастройкиМаркировкиУКЗ AS 
(
SELECT
	НастройкиМаркировки.КодТНВЭД,
	НастройкиМаркировки.ТоварнаяКатегория,
	НастройкиМаркировки.Номенклатура
FROM (SELECT
		T5._Fld32959RRef AS КодТНВЭД,
		T5._Fld32960RRef AS ТоварнаяКатегория,
		T5._Fld32961RRef AS Номенклатура,
		T5._Fld32962 AS МаркируетсяУКЗ
	FROM (SELECT
			T3._Fld32959RRef AS Fld32959RRef,
			T3._Fld32960RRef AS Fld32960RRef,
			T3._Fld32961RRef AS Fld32961RRef,
			MAX(T3._Period) AS MAXPERIOD_
		FROM dbo._InfoRg32958 T3
			INNER JOIN #Temp_Goods T5 WITH(NOLOCK)
			ON (T3._Fld32959RRef = T5.ТНВЭДСсылка)
		WHERE T3._Period <= @P_DateTimePeriodBegin
		GROUP BY T3._Fld32959RRef,
		T3._Fld32960RRef,
		T3._Fld32961RRef) T2
	INNER JOIN dbo._InfoRg32958 T5
		ON T2.Fld32959RRef = T5._Fld32959RRef 
		AND T2.Fld32960RRef = T5._Fld32960RRef 
		AND T2.Fld32961RRef = T5._Fld32961RRef 
		AND T2.MAXPERIOD_ = T5._Period) НастройкиМаркировки
WHERE НастройкиМаркировки.МаркируетсяУКЗ = 0x01),
ЕстьЗарегистрированныеШтрихкоды AS (
	SELECT DISTINCT
		ШтрихкодыНоменклатуры._Fld15621RRef AS Номенклатура
	FROM dbo._InfoRg15619 ШтрихкодыНоменклатуры WITH (NOLOCK)
		INNER JOIN #Temp_Goods Товары
		ON Товары.НоменклатураСсылка = ШтрихкодыНоменклатуры._Fld15621RRef
	WHERE ШтрихкодыНоменклатуры._Fld27279 = 0x1
)
SELECT TOP 1
	МаркируемыеТовары.НоменклатураСсылка
INTO #Temp_MarkedGoodsWithoutGtin
FROM
	(-- Маркировка УКЗ
	SELECT
		Товары.НоменклатураСсылка	
	FROM #Temp_Goods Товары
		INNER JOIN НастройкиМаркировкиУКЗ НастройкиПоНоменклатуре
		ON НастройкиПоНоменклатуре.КодТНВЭД = Товары.ТНВЭДСсылка
		AND НастройкиПоНоменклатуре.Номенклатура = Товары.НоменклатураСсылка
	
	UNION ALL 

	SELECT
		Товары.НоменклатураСсылка	
	FROM #Temp_Goods Товары
		INNER JOIN НастройкиМаркировкиУКЗ НастройкиПоТоварнойКатегории
		ON НастройкиПоТоварнойКатегории.КодТНВЭД = Товары.ТНВЭДСсылка
		AND НастройкиПоТоварнойКатегории.ТоварнаяКатегория = Товары.ТоварнаяКатегорияСсылка
		AND НастройкиПоТоварнойКатегории.ТоварнаяКатегория != 0x00000000000000000000000000000000
		LEFT JOIN НастройкиМаркировкиУКЗ НастройкиПоНоменклатуре
		ON НастройкиПоНоменклатуре.КодТНВЭД = Товары.ТНВЭДСсылка
		AND НастройкиПоНоменклатуре.Номенклатура = Товары.НоменклатураСсылка
	WHERE 
		НастройкиПоНоменклатуре.Номенклатура IS NULL

	UNION ALL 

	SELECT
		Товары.НоменклатураСсылка	
	FROM #Temp_Goods Товары
		INNER JOIN НастройкиМаркировкиУКЗ НастройкиПоКодуТНВЭД
		ON НастройкиПоКодуТНВЭД.КодТНВЭД = Товары.ТНВЭДСсылка
		AND НастройкиПоКодуТНВЭД.ТоварнаяКатегория = 0x00000000000000000000000000000000
		AND НастройкиПоКодуТНВЭД.Номенклатура = 0x00000000000000000000000000000000
		LEFT JOIN НастройкиМаркировкиУКЗ НастройкиПоТоварнойКатегории
		ON НастройкиПоТоварнойКатегории.КодТНВЭД = Товары.ТНВЭДСсылка
		AND НастройкиПоТоварнойКатегории.ТоварнаяКатегория = Товары.ТоварнаяКатегорияСсылка
		AND НастройкиПоТоварнойКатегории.ТоварнаяКатегория != 0x00000000000000000000000000000000
		LEFT JOIN НастройкиМаркировкиУКЗ НастройкиПоНоменклатуре
		ON НастройкиПоНоменклатуре.КодТНВЭД = Товары.ТНВЭДСсылка
		AND НастройкиПоНоменклатуре.Номенклатура = Товары.НоменклатураСсылка
	WHERE 
		НастройкиПоНоменклатуре.Номенклатура IS NULL
		AND НастройкиПоТоварнойКатегории.ТоварнаяКатегория IS NULL

	UNION ALL 

	--Маркировка СИ
	SELECT
		Товары.НоменклатураСсылка	
	FROM #Temp_Goods Товары
		INNER JOIN #Temp_MarkedCodes МаркируемыеКодыТНВЭД 
		ON МаркируемыеКодыТНВЭД.КодТНВЭД = Товары.ТНВЭДСсылка
		AND МаркируемыеКодыТНВЭД.ДополнительныйКод = Товары.ДопКодТНВЭД
		AND МаркируемыеКодыТНВЭД.ДополнительныйКод != 0x00000000000000000000000000000000

	UNION ALL 

	SELECT
		Товары.НоменклатураСсылка	
	FROM #Temp_Goods Товары
		INNER JOIN #Temp_MarkedCodes МаркируемыеКодыТНВЭД 
		ON МаркируемыеКодыТНВЭД.КодТНВЭД = Товары.ТНВЭДСсылка
		AND МаркируемыеКодыТНВЭД.ДополнительныйКод = 0x00000000000000000000000000000000

	UNION ALL 

	--Потенциальная маркировки СИ и УКЗ
	SELECT
		Товары.НоменклатураСсылка	
	FROM #Temp_Goods Товары
		INNER JOIN dbo._InfoRg33196 ПотенциальноМаркируемыеТоварныеКатегории 
		ON Товары.ТоварнаяКатегорияСсылка = ПотенциальноМаркируемыеТоварныеКатегории._Fld33197RRef
		AND Товары.ТНВЭДСсылка = 0x00000000000000000000000000000000
	WHERE 
		ПотенциальноМаркируемыеТоварныеКатегории._Fld33199 = 0x01
		OR ПотенциальноМаркируемыеТоварныеКатегории._Fld33200 = 0x01) МаркируемыеТовары
		LEFT JOIN ЕстьЗарегистрированныеШтрихкоды
		ON МаркируемыеТовары.НоменклатураСсылка = ЕстьЗарегистрированныеШтрихкоды.Номенклатура
WHERE 
	ЕстьЗарегистрированныеШтрихкоды.Номенклатура IS NULL
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Размеры корзины в целом для расчета габаритов*/
SELECT
CAST(SUM((T2._Fld6000 * T1.Количество)) AS NUMERIC(36, 6)) AS Вес,
CAST(SUM((T2._Fld6006 * T1.Количество)) AS NUMERIC(38, 8)) AS Объем,
MAX(T2._Fld6001) AS Высота,
MAX(T2._Fld6002) AS Глубина,
MAX(T2._Fld6009) AS Ширина,
0x00000000000000000000000000000000  AS Габарит
Into #Temp_Size
FROM #Temp_Goods T1 WITH(NOLOCK)
INNER JOIN dbo._Reference256 T2 With (NOLOCK) 
ON (T2._IDRRef = T1.УпаковкаСсылка) AND (T1.УпаковкаСсылка <> 0x00000000000000000000000000000000)
OPTION (KEEP PLAN, KEEPFIXED PLAN);
/*Габарит корзины общий*/
SELECT
    TOP 1 CASE
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
    INNER JOIN dbo._Const21167 T2 ON 1 = 1
    INNER JOIN dbo._Const21165 T3 ON 1 = 1
    INNER JOIN dbo._Const21338 T4 ON 1 = 1
    INNER JOIN dbo._Const21336 T5 ON 1 = 1
    INNER JOIN dbo._Const21579 T6 ON 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

SELECT
    COUNT_BIG(T1.НоменклатураСсылка) AS КоличествоСтрок,
    T1.НоменклатураСсылка AS НоменклатураСсылка,
    T2._Fld6000 * T1.Количество AS Вес,
    T2._Fld6006 * T1.Количество AS Объем,
    SUM(CASE 
        WHEN MarkedCodes.КодТНВЭД IS NOT NULL AND T1.Количество >= 5
            THEN T1.Количество * T3.ДополнительноеВремяМаркируемыеТовары
        ELSE 0
    END) AS УсловиеПоМаркируемымТоварам,		
    CASE
        WHEN (
            (T2._Fld6006 > 0.8)
            OR (T2._Fld6002 > 1.85)
            OR (T2._Fld6001 > 1.85)
            OR (T2._Fld6009 > 1.85)
        )
        AND (T2._Fld6000 < 120.0)
        AND (T2._Fld6000 >= 50.0) THEN (T3.Fld24101_ * @P_Floor)
        WHEN (
            (T2._Fld6006 > 0.8)
            OR (T2._Fld6002 > 1.85)
            OR (T2._Fld6001 > 1.85)
            OR (T2._Fld6009 > 1.85)
        )
        AND (T2._Fld6000 >= 120.0) THEN (T3.Fld24102_ * @P_Floor)
        WHEN (
            (T2._Fld6006 > 0.8)
            OR (T2._Fld6002 > 1.85)
            OR (T2._Fld6001 > 1.85)
            OR (T2._Fld6009 > 1.85)
        )
        AND (T2._Fld6000 < 5.0) THEN (T3.Fld26615_ * @P_Floor)
        WHEN (
            (T2._Fld6006 > 0.8)
            OR (T2._Fld6002 > 1.85)
            OR (T2._Fld6001 > 1.85)
            OR (T2._Fld6009 > 1.85)
        )
        AND (T2._Fld6000 >= 5.0)
        AND (T2._Fld6000 < 50.0) THEN (T3.Fld26616_ * @P_Floor)
        ELSE 0.0
    END  AS УсловиеЭтажМассаПоТоварам
Into #Temp_Weight
FROM
    #Temp_Goods T1 WITH(NOLOCK)
    LEFT OUTER JOIN dbo._Reference256 T2 With (NOLOCK) ON (
        0x08 = T2._OwnerID_TYPE
        AND 0x00000095 = T2._OwnerID_RTRef
        AND T1.УпаковкаСсылка = T2._IDRRef
    )
    INNER JOIN (
        SELECT
            T6._Fld24101 AS Fld24101_,
            T6._Fld24102 AS Fld24102_,
            T6._Fld26615 AS Fld26615_,
            T6._Fld30450 AS ДополнительноеВремяМаркируемыеТовары,
            T6._Fld26616 AS Fld26616_
        FROM
            (
                SELECT
                    MAX(T5._Period) AS MAXPERIOD_
                FROM
                    dbo._InfoRg24088 T5
            ) T4
            INNER JOIN dbo._InfoRg24088 T6 ON T4.MAXPERIOD_ = T6._Period
    ) T3 ON 1 = 1
    LEFT OUTER JOIN #Temp_MarkedCodes AS MarkedCodes
    ON MarkedCodes.КодТНВЭД = T1.ТНВЭДСсылка
    And (MarkedCodes.ДополнительныйКод = T1.ДопКодТНВЭД
		Or MarkedCodes.ДополнительныйКод = 0x00000000000000000000000000000000)
GROUP BY
    T1.НоменклатураСсылка,
    T2._Fld6000 * T1.Количество,
    T2._Fld6006 * T1.Количество,
    CASE
        WHEN (
            (T2._Fld6006 > 0.8)
            OR (T2._Fld6002 > 1.85)
            OR (T2._Fld6001 > 1.85)
            OR (T2._Fld6009 > 1.85)
        )
        AND (T2._Fld6000 < 120.0)
        AND (T2._Fld6000 >= 50.0) THEN (T3.Fld24101_ * @P_Floor)
        WHEN (
            (T2._Fld6006 > 0.8)
            OR (T2._Fld6002 > 1.85)
            OR (T2._Fld6001 > 1.85)
            OR (T2._Fld6009 > 1.85)
        )
        AND (T2._Fld6000 >= 120.0) THEN (T3.Fld24102_ * @P_Floor)
        WHEN (
            (T2._Fld6006 > 0.8)
            OR (T2._Fld6002 > 1.85)
            OR (T2._Fld6001 > 1.85)
            OR (T2._Fld6009 > 1.85)
        )
        AND (T2._Fld6000 < 5.0) THEN (T3.Fld26615_ * @P_Floor)
        WHEN (
            (T2._Fld6006 > 0.8)
            OR (T2._Fld6002 > 1.85)
            OR (T2._Fld6001 > 1.85)
            OR (T2._Fld6009 > 1.85)
        )
        AND (T2._Fld6000 >= 5.0)
        AND (T2._Fld6000 < 50.0) THEN (T3.Fld26616_ * @P_Floor)
        ELSE 0.0
    END
OPTION (KEEP PLAN, KEEPFIXED PLAN);

SELECT Distinct
    CASE
        WHEN (IsNull(T2.Габарит,0x8AB421D483ABE88A4C4C9928262FFB0D) = 0x8AB421D483ABE88A4C4C9928262FFB0D) THEN 7 --мбт
        ELSE 14
    END AS УсловиеГабариты,
    CASE
        WHEN (@P_Credit = 1) --кредит рассрочка
            THEN T3.Fld24103_
        ELSE 0
    END AS УсловиеСпособОплаты,
    T1.КоэффициентЗоныДоставки,
    CASE
        WHEN (T1.ЗонаДоставкиРодительНаименование LIKE '%Минск%') --наименование зоны доставки
        AND (IsNull(T2.Габарит,0x8AB421D483ABE88A4C4C9928262FFB0D) = 0x8AB421D483ABE88A4C4C9928262FFB0D) THEN T3.Fld24091_ --мбт
        WHEN (T1.ЗонаДоставкиРодительНаименование LIKE '%Минск%')
        AND (IsNull(T2.Габарит,0x8AB421D483ABE88A4C4C9928262FFB0D) = 0xAD3F7F5FC4F15DAD4F693CAF8365EC0D) THEN T3.Fld24092_ --кбт
        ELSE 0
    END AS УсловиеМинскЧас
Into #Temp_TimeByOrders
FROM
    #Temp_GeoData T1 WITH(NOLOCK)
	Left Join #Temp_Dimensions T2 On 1=1
    INNER JOIN (
        SELECT
            T5._Fld24103 AS Fld24103_,
            T5._Fld24091 AS Fld24091_,
            T5._Fld24092 AS Fld24092_
        FROM
            (
                SELECT
                    MAX(T4._Period) AS MAXPERIOD_
                FROM
                    dbo._InfoRg24088 T4
            ) T3
            INNER JOIN dbo._InfoRg24088 T5 ON T3.MAXPERIOD_ = T5._Period
    ) T3 ON 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select 
	SUM(T1.КоличествоСтрок) AS КоличествоСтрок,
	SUM(Case when T1.УсловиеЭтажМассаПоТоварам = 0 then T1.Вес else 0 end) AS Вес,
	SUM(Case when T1.УсловиеЭтажМассаПоТоварам = 0 then T1.Объем else 0 end) AS Объем,
	SUM(T1.УсловиеЭтажМассаПоТоварам) AS УсловиеЭтажМассаОбщ,
	SUM(T1.УсловиеПоМаркируемымТоварам) AS УсловиеПоМаркируемымТоварам
Into #Temp_TotalWeight
From 
	#Temp_Weight T1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T2.Fld24090_ * T1.КоличествоСтрок AS УсловиеКоличествоСтрок,
    CASE
        WHEN T1.Объем = 0
			AND T1.Вес = 0 THEN 0
        WHEN T1.Объем < 0.8
			AND T1.Вес < 5.0 THEN T2.Fld24094_
        WHEN T1.Объем < 0.8
			AND T1.Вес >= 5.0
			AND T1.Вес < 20.0 THEN T2.Fld24095_
        WHEN (T1.Объем) < 0.8
			AND T1.Вес >= 20.0
			AND T1.Вес < 65.0 THEN T2.Fld24096_
        WHEN T1.Объем < 0.8
			AND T1.Вес >= 65.0
			AND T1.Вес < 120.0 THEN T2.Fld24097_
        WHEN T1.Объем < 0.8
			AND T1.Вес >= 120.0
			AND T1.Вес < 250.0 THEN T2.Fld24098_
        WHEN T1.Объем < 0.8
			AND T1.Вес >= 250.0
			AND T1.Вес < 400.0 THEN T2.Fld26611_
        WHEN T1.Объем < 0.8
			AND T1.Вес >= 400.0 THEN T2.Fld26612_
        WHEN T1.Объем >= 0.8
			AND T1.Вес < 120.0 THEN T2.Fld24099_
        WHEN T1.Объем >= 0.8
			AND T1.Вес >= 120.0
			AND T1.Вес < 250.0 THEN T2.Fld24100_
        WHEN T1.Объем >= 0.8
			AND T1.Вес >= 250.0
			AND T1.Вес < 600.0 THEN T2.Fld26613_
        WHEN T1.Объем >= 0.8
			AND T1.Вес >= 600.0 THEN T2.Fld26614_
    END As УсловиеВесОбъем,
    T2.Fld24089_ As МинимальноеВремя,
    T1.УсловиеПоМаркируемымТоварам AS УсловиеПоМаркируемымТоварам,
    T1.УсловиеЭтажМассаОбщ As УсловиеЭтажМассаОбщ
INTO #Temp_Time1
FROM
    #Temp_TotalWeight T1 WITH(NOLOCK)
    INNER JOIN (
        SELECT
            T5._Fld24090 AS Fld24090_,
            T5._Fld24094 AS Fld24094_,
            T5._Fld24095 AS Fld24095_,
            T5._Fld24096 AS Fld24096_,
            T5._Fld24097 AS Fld24097_,
            T5._Fld24098 AS Fld24098_,
            T5._Fld26611 AS Fld26611_,
            T5._Fld26612 AS Fld26612_,
            T5._Fld24099 AS Fld24099_,
            T5._Fld24100 AS Fld24100_,
            T5._Fld26613 AS Fld26613_,
            T5._Fld26614 AS Fld26614_,
            T5._Fld24089 AS Fld24089_
        FROM
            (
                SELECT
                    MAX(T4._Period) AS MAXPERIOD_
                FROM
                    dbo._InfoRg24088 T4
            ) T3
            INNER JOIN dbo._InfoRg24088 T5 ON T3.MAXPERIOD_ = T5._Period
    ) T2 ON 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Время обслуживания началось выше и тут итоговая цифра*/
SELECT
    (ISNULL(T2.МинимальноеВремя, 0) 
	+ ISNULL(T2.УсловиеКоличествоСтрок, 0) 
	+ ISNULL(T1.УсловиеМинскЧас, 0) 
	+ ISNULL(T2.УсловиеЭтажМассаОбщ, 0) 
	+ ISNULL(T2.УсловиеВесОбъем, 0) 
	+ ISNULL(T1.УсловиеСпособОплаты, 0) 
	+ ISNULL(T2.УсловиеПоМаркируемымТоварам, 0)) * T1.КоэффициентЗоныДоставки
	AS ВремяВыполнения
Into #Temp_TimeService
FROM
    #Temp_TimeByOrders T1 WITH(NOLOCK)
    LEFT OUTER JOIN #Temp_Time1 T2 WITH(NOLOCK)
    ON 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

/*Группа планирования*/
Select ГруппыПланирования._IDRRef AS ГруппаПланирования,
	ГруппыПланирования._Fld23302RRef AS Склад,
    CASE WHEN @P_YourTimeDelivery = 1
		THEN ГруппыПланирования._Fld30397
		ELSE ГруппыПланирования._Fld25137
	END AS ВремяДоступностиНаСегодня,
    ГруппыПланирования._Fld25138 AS ВремяСтопаСегодня,
	ГруппыПланирования._Fld25139 AS ВремяДоступностиНаЗавтра,
	ГруппыПланирования._Fld25140 AS ВремяСтопаЗавтра,
    DATEDIFF(SECOND, @P_EmptyDate, CASE WHEN @P_YourTimeDelivery = 1
		THEN ГруппыПланирования._Fld30399
		ELSE ГруппыПланирования._Fld25519
	END) AS ВремяНаПодготовкуПоставщик,
	DATEDIFF(SECOND, @P_EmptyDate, ГруппыПланирования._Fld25520) AS ВремяНаПодготовкуПеремещение,
    CAST(DATEDIFF(SECOND, @P_EmptyDate, CASE WHEN @P_YourTimeDelivery = 1
		THEN ГруппыПланирования._Fld30398
		ELSE ГруппыПланирования._Fld25132
	END) / 60.0 AS NUMERIC(15, 2)) AS СреднееВремяНаПереезд,
    1 AS Основная,
	ГруппыПланирования._Description
Into #Temp_PlanningGroups
From
dbo._Reference23294 ГруппыПланирования With (NOLOCK)
	Inner Join dbo._Reference23294_VT23309 With (NOLOCK)
		on ГруппыПланирования._IDRRef = _Reference23294_VT23309._Reference23294_IDRRef
		and _Reference23294_VT23309._Fld23311RRef in (Select ЗонаДоставкиРодительСсылка From #Temp_GeoData)
	AND ГруппыПланирования._Fld25141 = 0x01--участвует в расчете мощности
	AND ГруппыПланирования._Fld23301RRef IN (Select Габарит From #Temp_Dimensions With (NOLOCK))  --габариты
	AND ГруппыПланирования._Marked = 0x00
    AND CASE WHEN @P_YourTimeDelivery = 1
		THEN
			ГруппыПланирования._Fld30395 --участвует в доставке в ваше время
		ELSE 0x01
		END = 0x01		
UNION ALL
Select 
	ПодчиненнаяГП._IDRRef AS ГруппаПланирования,
	ГруппыПланирования._Fld23302RRef AS Склад,
    CASE WHEN @P_YourTimeDelivery = 1
		THEN ПодчиненнаяГП._Fld30397
		ELSE ПодчиненнаяГП._Fld25137
	END AS ВремяДоступностиНаСегодня,
    ПодчиненнаяГП._Fld25138 AS ВремяСтопаСегодня,
	ПодчиненнаяГП._Fld25139 AS ВремяДоступностиНаЗавтра,
	ПодчиненнаяГП._Fld25140 AS ВремяСтопаЗавтра,
	DATEDIFF(SECOND, @P_EmptyDate, CASE WHEN @P_YourTimeDelivery = 1
		THEN ПодчиненнаяГП._Fld30399
		ELSE ПодчиненнаяГП._Fld25519
	END),
	DATEDIFF(SECOND, @P_EmptyDate, ПодчиненнаяГП._Fld25520),
    CAST(DATEDIFF(SECOND, @P_EmptyDate, CASE WHEN @P_YourTimeDelivery = 1
		THEN ПодчиненнаяГП._Fld30398
		ELSE ПодчиненнаяГП._Fld25132
	END) / 60.0 AS NUMERIC(15, 2)) AS СреднееВремяНаПереезд,
    0,
	ПодчиненнаяГП._Description
From
	dbo._Reference23294 ГруппыПланирования With (NOLOCK)
	Inner Join dbo._Reference23294_VT23309	With (NOLOCK)	
		on ГруппыПланирования._IDRRef = _Reference23294_VT23309._Reference23294_IDRRef
		and _Reference23294_VT23309._Fld23311RRef in (Select ЗонаДоставкиРодительСсылка From #Temp_GeoData)
	Inner Join dbo._Reference23294 ПодчиненнаяГП
			On  ГруппыПланирования._Fld26526RRef = ПодчиненнаяГП._IDRRef
                AND CASE WHEN @P_YourTimeDelivery = 1
					THEN
						ПодчиненнаяГП._Fld30395 --участвует в доставке в ваше время
					ELSE 0x01
					END = 0x01		
Where 
	ГруппыПланирования._Fld25141 = 0x01--участвует в расчете мощности
	AND ГруппыПланирования._Marked = 0x00
OPTION (KEEP PLAN, KEEPFIXED PLAN)
;

/*Отсюда начинается процесс получения оптимальной даты отгрузки*/
With Temp_ExchangeRates AS (
SELECT
	T1._Period AS Период,
	T1._Fld14558RRef AS Валюта,
	T1._Fld14559 AS Курс,
	T1._Fld14560 AS Кратность
FROM _InfoRgSL26678 T1 With (NOLOCK)
	)
SELECT
    T2._Fld21408RRef AS НоменклатураСсылка,
    T2._Fld21410_TYPE AS Источник_TYPE,
	T2._Fld21410_RTRef AS Источник_RTRef,
	T2._Fld21410_RRRef AS Источник_RRRef,
	Цены._Fld21410_TYPE AS Регистратор_TYPE,
    Цены._Fld21410_RTRef AS Регистратор_RTRef,
    Цены._Fld21410_RRRef AS Регистратор_RRRef,
    T2._Fld23568RRef AS СкладИсточника,
    T2._Fld21424 AS ДатаСобытия,
    SUM(T2._Fld21411) - SUM(T2._Fld21412) AS Количество
Into #Temp_Remains
FROM
    dbo._AccumRgT21444 T2 With (NOLOCK)
	Left Join _AccumRg21407 Цены With (NOLOCK)
		Inner Join Temp_ExchangeRates With (NOLOCK)
			On Цены._Fld21443RRef = Temp_ExchangeRates.Валюта 
		On T2._Fld21408RRef = Цены._Fld21408RRef
		AND T2._Fld21410_RTRef = 0x00000153
		AND Цены._Fld21410_RTRef = 0x00000153 --Цены.Регистратор ССЫЛКА Документ.мегапрайсРегистрацияПрайса
		AND T2._Fld21410_RRRef = Цены._Fld21410_RRRef
        And Цены._Fld30969 = 0x00 -- ОтказПоФильтруЦен = Ложь
        --And (Цены._Fld21982<>0 AND Цены._Fld21442 * Temp_ExchangeRates.Курс / Temp_ExchangeRates.Кратность >= Цены._Fld21982 OR Цены._Fld21411 >= Цены._Fld21616)
		And Цены._Fld21408RRef IN(SELECT
                НоменклатураСсылка
            FROM
                #Temp_Goods)
WHERE
    T2._Period = '5999-11-01 00:00:00'
    AND (
        (
            (T2._Fld21424 = '2001-01-01 00:00:00')
            OR (Cast(T2._Fld21424 AS datetime)>= @P_DateTimeNow)
        )
        AND T2._Fld21408RRef IN (
            SELECT
                TNomen.НоменклатураСсылка
            FROM
                #Temp_Goods TNomen WITH(NOLOCK))) AND (T2._Fld21412 <> 0 OR T2._Fld21411 <> 0)
GROUP BY
    T2._Fld21408RRef,
    T2._Fld21410_TYPE,
    T2._Fld21410_RTRef,
    T2._Fld21410_RRRef,
	Цены._Fld21410_TYPE,
    Цены._Fld21410_RTRef,
    Цены._Fld21410_RRRef,
    T2._Fld23568RRef,
    T2._Fld21424
HAVING
    (SUM(T2._Fld21412) <> 0.0
    OR SUM(T2._Fld21411) <> 0.0)
	AND SUM(T2._Fld21411) - SUM(T2._Fld21412) > 0.0
Union ALL
SELECT --товары по заказу
    Резервирование._Fld21408RRef AS НоменклатураСсылка,
    Резервирование._Fld21410_TYPE AS Источник_TYPE,
	Резервирование._Fld21410_RTRef AS Источник_RTRef,
	Резервирование._Fld21410_RRRef AS Источник_RRRef,
	0x08 AS Регистратор_TYPE,
    Резервирование._RecorderTRef AS Регистратор_RTRef,
    Резервирование._RecorderRRef AS Регистратор_RRRef,
    Резервирование._Fld23568RRef AS СкладИсточника,
    Резервирование._Fld21424 AS ДатаСобытия,
    Резервирование._Fld21411 - Резервирование._Fld21412 AS Количество
FROM
	_AccumRg21407 Резервирование With (NOLOCK)
	Inner Join #Temp_GoodsOrder On
		Резервирование._RecorderRRef = #Temp_GoodsOrder.ЗаказСсылка
		And Резервирование._Fld21408RRef = #Temp_GoodsOrder.НоменклатураСсылка
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
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With SourceWarehouses AS
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
    dbo._InfoRg23830 T1 With (NOLOCK{6}) 
    Inner Join SourceWarehouses On T1._Fld23831RRef = SourceWarehouses.СкладИсточника	
WHERE
	T1._Fld23833RRef IN (Select СкладСсылка From #Temp_GeoData UNION ALL Select СкладСсылка From #Temp_PickupPoints)
		AND	T1._Fld23832 BETWEEN @P_DateTimeNow AND DateAdd(DAY,6,@P_DateTimeNow)
GROUP BY T1._Fld23831RRef,
T1._Fld23833RRef
OPTION (HASH GROUP, OPTIMIZE FOR (@P_DateTimeNow='{1}'), KEEP PLAN, KEEPFIXED PLAN);


SELECT
    T1.НоменклатураСсылка,
    T1.Количество,
    T1.Источник_TYPE,
    T1.Источник_RTRef,
    T1.Источник_RRRef,
    T1.СкладИсточника,
    T1.ДатаСобытия,
    DATEADD(SECOND, 
		CASE WHEN ISNULL(T3.СкладНазначения, T2.СкладНазначения) <> T1.СкладИсточника 
			THEN ISNULL(#Temp_PlanningGroups.ВремяНаПодготовкуПеремещение, 0) 
			ELSE 0 
		END, 
		ISNULL(T3.ДатаПрибытия, T2.ДатаПрибытия)) AS ДатаДоступности,
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
    LEFT JOIN #Temp_PlanningGroups 
	ON ISNULL(T3.СкладНазначения, T2.СкладНазначения) = #Temp_PlanningGroups.Склад 
		AND #Temp_PlanningGroups.Основная = 1
WHERE
    T1.Количество > 0 And
    T1.Источник_RTRef = 0x000000E2 OR T1.Источник_RTRef = 0x00000150 --склад, корректировка регистров

UNION ALL

SELECT
    T4.НоменклатураСсылка,
    T4.Количество,
    T4.Источник_TYPE,
    T4.Источник_RTRef,
    T4.Источник_RRRef,
    T4.СкладИсточника,
    T4.ДатаСобытия,
	DATEADD(SECOND, 
		CASE WHEN T5.СкладНазначения = T4.СкладИсточника 
			THEN ISNULL(#Temp_PlanningGroups.ВремяНаПодготовкуПоставщик, 0) 
			ELSE ISNULL(#Temp_PlanningGroups.ВремяНаПодготовкуПеремещение, 0) 
		END, 
		T5.ДатаПрибытия),
    2,
    1,
    T5.СкладНазначения
FROM
    #Temp_Remains T4 WITH(NOLOCK)
    INNER JOIN #Temp_WarehouseDates T5 WITH(NOLOCK)
    ON (T4.СкладИсточника = T5.СкладИсточника)
    AND (T4.ДатаСобытия = T5.ДатаСобытия)
	LEFT JOIN #Temp_PlanningGroups 
    ON T5.СкладНазначения = #Temp_PlanningGroups.Склад 
        AND #Temp_PlanningGroups.Основная = 1
WHERE
    T4.Количество > 0 And
    T4.Источник_RTRef = 0x00000141 --заказ поставщику

UNION ALL

SELECT
    T6.НоменклатураСсылка,
    T6.Количество,
    T6.Источник_TYPE,
    T6.Источник_RTRef,
    T6.Источник_RRRef,
    T6.СкладИсточника,
    T6.ДатаСобытия,
	DATEADD(SECOND, 
		CASE WHEN T7.СкладНазначения = T6.СкладИсточника 
			THEN ISNULL(#Temp_PlanningGroups.ВремяНаПодготовкуПоставщик, 0) 
			ELSE ISNULL(#Temp_PlanningGroups.ВремяНаПодготовкуПеремещение, 0) 
		END, 
		T7.ДатаПрибытия),
    3,
    0,
    T7.СкладНазначения
FROM
    #Temp_Remains T6 WITH(NOLOCK)
    INNER JOIN #Temp_WarehouseDates T7 WITH(NOLOCK)
    ON (T6.СкладИсточника = T7.СкладИсточника)
    AND (T6.ДатаСобытия = T7.ДатаСобытия)
	LEFT JOIN #Temp_PlanningGroups With (NOLOCK) 
    ON T7.СкладНазначения = #Temp_PlanningGroups.Склад 
        AND #Temp_PlanningGroups.Основная = 1
WHERE
    T6.Количество > 0 And
    NOT T6.Регистратор_RRRef IS NULL
	And T6.Источник_RTRef = 0x00000153 --мегапрайсРегистрацияПрайса

UNION ALL

Select
	векРезервированиеТоваров._Fld21408RRef,
	векРезервированиеТоваров._Fld21412,
	векРезервированиеТоваров._Fld21410_TYPE,
	векРезервированиеТоваров._Fld21410_RTRef,
	векРезервированиеТоваров._Fld21410_RRRef,
	векРезервированиеТоваров._Fld23568RRef,
	векРезервированиеТоваров._Fld21424,
	DATEADD(SECOND, 
		CASE WHEN ISNULL(#Temp_WarehouseDates.СкладНазначения, #Temp_MinimumWarehouseDates.СкладНазначения) = векРезервированиеТоваров._Fld23568RRef
			THEN CASE WHEN векРезервированиеТоваров._Fld21410_RTRef = 0x00000141 OR векРезервированиеТоваров._Fld21410_RTRef = 0x00000153 -- Заказ поствщику или мегапрайс
				THEN ISNULL(#Temp_PlanningGroups.ВремяНаПодготовкуПоставщик, 0)
				ELSE 0
			END
			ELSE ISNULL(#Temp_PlanningGroups.ВремяНаПодготовкуПеремещение, 0)
		END, 
		ISNULL(#Temp_WarehouseDates.ДатаПрибытия, #Temp_MinimumWarehouseDates.ДатаПрибытия)),
	4,
    CASE 
		WHEN Товары.Склад = векРезервированиеТоваров._Fld21410_RRRef
			THEN 1
		ELSE 0
	END,
    ISNULL(#Temp_WarehouseDates.СкладНазначения, #Temp_MinimumWarehouseDates.СкладНазначения) AS СкладНазначения
From
	dbo._AccumRg21407 векРезервированиеТоваров With (NOLOCK)
		Inner Join  #Temp_GoodsOrder Товары
		ON (векРезервированиеТоваров._RecorderRRef = Товары.ЗаказСсылка)		
			AND (векРезервированиеТоваров._RecorderTRef = 0x0000013D) --поменять на правильный тип ЗаказКлиента 
				--OR векРезервированиеТоваров._RecorderTRef = 0x00000153)
			AND векРезервированиеТоваров._Fld21408RRef = Товары.НоменклатураСсылка --номенклатура
			AND (векРезервированиеТоваров._Fld21410_RRRef <> 0x00000000000000000000000000000000) 
		Left Join #Temp_WarehouseDates
		ON векРезервированиеТоваров._Fld23568RRef = #Temp_WarehouseDates.СкладИсточника
			AND векРезервированиеТоваров._Fld21424 = #Temp_WarehouseDates.ДатаСобытия
		Left Join #Temp_MinimumWarehouseDates 
		On векРезервированиеТоваров._Fld23568RRef = #Temp_MinimumWarehouseDates.СкладИсточника
			AND векРезервированиеТоваров._Fld21424 = '2001-01-01 00:00:00'
		Left Join #Temp_PlanningGroups With (NOLOCK) 
		On ISNULL(#Temp_WarehouseDates.СкладНазначения, #Temp_MinimumWarehouseDates.СкладНазначения) = #Temp_PlanningGroups.Склад 
			AND #Temp_PlanningGroups.Основная = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With TempSourcesGrouped AS
(
Select
	T1.НоменклатураСсылка AS НоменклатураСсылка,
    Min(Case When T1.ЭтоСклад = 1 Then T1.ДатаДоступности Else @P_MaxDate End) AS ДатаДоступностиСклад,
	Sum(Case When T1.ЭтоСклад = 1 Then T1.Количество Else 0 End) AS ОстатокНаСкладе,
	Sum(T1.Количество) AS ОстатокВсего
From
	(Select 
		T2.НоменклатураСсылка,
		T2.ЭтоСклад,
		Min(T2.ДатаДоступности) As ДатаДоступности,
		T2.Количество
	From #Temp_Sources T2
	Group by
	    T2.НоменклатураСсылка,
	    T2.Источник_RRRef,
	    T2.ЭтоСклад,
	    T2.Количество) T1
Group by
	T1.НоменклатураСсылка
)
Select
	T1.НоменклатураСсылка,
	isNull(T2.ДатаДоступностиСклад, @P_MaxDate) AS ДатаДоступностиСклад,
	min(Case when T1.Количество <= isNull(T2.ОстатокВсего, 0) Then 1 Else 0 End) As ОстаткаДостаточно,
	min(Case when T1.Количество <= isNull(T2.ОстатокНаСкладе, 0) Then 1 Else 0 End) As ОстаткаНаСкладеДостаточно,
	min(Case when isNull(T2.ОстатокНаСкладе, 0) > 0 Then 1 Else 0 End) As ОстатокЕсть
Into #Temp_StockSourcesAvailable
From #Temp_Goods T1
	left join TempSourcesGrouped T2
	on T1.НоменклатураСсылка = T2.НоменклатураСсылка
Where @P_StockPriority = 1
Group by
    isNull(T2.ДатаДоступностиСклад, @P_MaxDate),
    T1.НоменклатураСсылка
Having 
    min(Case when 0 < isNull(T2.ОстатокНаСкладе, 0) Then 1 Else 0 End) = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);


With TempSourcesGrouped AS
(
Select
	T1.НоменклатураСсылка AS НоменклатураСсылка,
	T1.ЭтоСклад,
	Sum(T1.Количество) AS Количество,
	T1.ДатаДоступности AS ДатаДоступности,
	T1.СкладНазначения AS СкладНазначения
From
	#Temp_Sources T1	
Group by
	T1.НоменклатураСсылка,
	T1.ЭтоСклад,
	T1.ДатаДоступности,
	T1.СкладНазначения
)
Select
	Источники1.НоменклатураСсылка AS Номенклатура,
	Источники1.СкладНазначения AS СкладНазначения,
	Источники1.ДатаДоступности AS ДатаДоступности,
	Min(#Temp_StockSourcesAvailable.ОстаткаНаСкладеДостаточно) AS ОстаткаНаСкладеДостаточно,
	Sum(Источник2.Количество) AS Количество
Into #Temp_AvailableGoods
From
	TempSourcesGrouped AS Источники1
		Left Join TempSourcesGrouped AS Источник2
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

Union all

Select
	Источники1.НоменклатураСсылка AS Номенклатура,
	Источники1.СкладНазначения AS СкладНазначения,
	Источники1.ДатаДоступности AS ДатаДоступности,
    1,
	Sum(Источник2.Количество) AS Количество
From
	TempSourcesGrouped AS Источники1
		Left Join TempSourcesGrouped AS Источник2
		On Источники1.НоменклатураСсылка = Источник2.НоменклатураСсылка
		AND Источники1.СкладНазначения = Источник2.СкладНазначения
			AND Источники1.ДатаДоступности >= Источник2.ДатаДоступности	
		Left Join #Temp_StockSourcesAvailable
		On Источники1.НоменклатураСсылка = #Temp_StockSourcesAvailable.НоменклатураСсылка
Where @P_StockPriority = 0
    Or #Temp_StockSourcesAvailable.ОстатокЕсть is null
Group by
	Источники1.НоменклатураСсылка,
	Источники1.ДатаДоступности,
	Источники1.СкладНазначения
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With Temp_ExchangeRates AS (
SELECT
	T1._Period AS Период,
	T1._Fld14558RRef AS Валюта,
	T1._Fld14559 AS Курс,
	T1._Fld14560 AS Кратность
FROM _InfoRgSL26678 T1 With (NOLOCK)
)
SELECT
    T1.НоменклатураСсылка,
	T1.Количество,
    T1.Источник_TYPE,
    T1.Источник_RTRef,
    T1.Источник_RRRef,
    T1.СкладИсточника,
    T1.СкладНазначения,
    T1.ДатаСобытия,
    T1.ДатаДоступности,
    CAST(
        (
            CAST(
                (Резервирование._Fld21442 * T3.Курс) AS NUMERIC(27, 8)
            ) / T3.Кратность
        ) AS NUMERIC(15, 2)
    )  AS Цена
Into #Temp_SourcesWithPrices
FROM
    #Temp_Sources T1 WITH(NOLOCK)
    INNER JOIN dbo._AccumRg21407 Резервирование WITH(NOLOCK)
    LEFT OUTER JOIN Temp_ExchangeRates T3 WITH(NOLOCK)
    ON (Резервирование._Fld21443RRef = T3.Валюта) 
    ON (T1.НоменклатураСсылка = Резервирование._Fld21408RRef)
    AND (
        T1.Источник_TYPE = 0x08
        AND T1.Источник_RTRef = Резервирование._RecorderTRef
        AND T1.Источник_RRRef = Резервирование._RecorderRRef
    )
OPTION (KEEP PLAN, KEEPFIXED PLAN, maxdop 4);

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
    DATEADD(DAY, {4}, T1.ДатаДоступности)--это параметр КоличествоДнейАнализа
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

SELECT
    T1.НоменклатураСсылка,
	T1.Количество,
    T1.Источник_TYPE,
    T1.Источник_RTRef,
    T1.Источник_RRRef,
    T1.СкладИсточника,
    T1.СкладНазначения,
    T1.ДатаСобытия,
    ISNULL(T2.ДатаДоступности1, T1.ДатаДоступности) AS ДатаДоступности,
    T1.ТипИсточника
Into #Temp_SourcesCorrectedDate
FROM
    #Temp_Sources T1 WITH(NOLOCK)
    LEFT OUTER JOIN #Temp_BestPriceAfterClosestDate T2 WITH(NOLOCK)
    ON (T1.НоменклатураСсылка = T2.НоменклатураСсылка)
    AND (T1.ДатаДоступности = T2.ДатаДоступности)
    AND (T1.СкладНазначения = T2.СкладНазначения)
    AND (T1.ТипИсточника = 3)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With Temp_ClosestDate AS
(
SELECT
    T1.НоменклатураСсылка,
    T1.СкладНазначения,
    MIN(T1.ДатаДоступности) AS ДатаДоступности
FROM 
    #Temp_Sources T1 WITH(NOLOCK)
GROUP BY 
    T1.НоменклатураСсылка,
    T1.СкладНазначения
)
Select 
	T4.НоменклатураСсылка AS НоменклатураСсылка,
	T4.СкладНазначения AS СкладНазначения,
	Min(T4.БлижайшаяДата) AS БлижайшаяДата
into #Temp_ClosestDatesByGoods
From 
(
Select 
	#Temp_Goods.НоменклатураСсылка AS НоменклатураСсылка,
	#Temp_AvailableGoods.СкладНазначения AS СкладНазначения,
	Min(IsNull(ЛучшиеЦеныПрайсов.ДатаДоступности, #Temp_AvailableGoods.ДатаДоступности)) AS БлижайшаяДата
From #Temp_Goods With (NOLOCK)
	Left Join #Temp_AvailableGoods With (NOLOCK) 
		On #Temp_Goods.НоменклатураСсылка = #Temp_AvailableGoods.Номенклатура
		AND #Temp_Goods.Количество <= #Temp_AvailableGoods.Количество
    Left Join #Temp_SourcesCorrectedDate ЛучшиеЦеныПрайсов With (NOLOCK)
		On ЛучшиеЦеныПрайсов.НоменклатураСсылка = #Temp_AvailableGoods.Номенклатура
		And ЛучшиеЦеныПрайсов.СкладНазначения = #Temp_AvailableGoods.СкладНазначения
		And #Temp_AvailableGoods.ОстаткаНаСкладеДостаточно = 0 -- только для товаров, которых нет на складе и в пути
		And #Temp_Goods.Количество = 1 -- Применяем смещение даты только для источников прайсов и количества = 1
Group By
	#Temp_Goods.НоменклатураСсылка,
	#Temp_AvailableGoods.СкладНазначения) T4
Group by 
	T4.НоменклатураСсылка,
	T4.СкладНазначения
OPTION (HASH GROUP, KEEP PLAN, KEEPFIXED PLAN);

Select
	СмещениеДатДоставки._Fld25220RRef As Склад,
	СмещениеДатДоставки._Fld25221 As РазмерСмещения
Into #Temp_PickupDateShift
From dbo._InfoRg25217 СмещениеДатДоставки With (NOLOCK)
	Inner Join #Temp_PickupPoints PickupPoints
	On PickupPoints.СкладСсылка = СмещениеДатДоставки._Fld25220RRef
	And СмещениеДатДоставки._Fld25218 <= @P_DateTimeNow
	And СмещениеДатДоставки._Fld25219 >= @P_DateTimeNow
OPTION (KEEP PLAN, KEEPFIXED PLAN);

With Temp_CountOfGoods AS
(
SELECT
    COUNT(DISTINCT T1.НоменклатураСсылка) as CountOfGoods
FROM 
    #Temp_Goods T1 WITH(NOLOCK)
)
Select Top 1 
	Max(DATEADD(HOUR, ISNULL(СмещениеДатДоставки.РазмерСмещения, 0), БлижайщиеДатыПоТоварам.БлижайшаяДата)) AS DateAvailable, 
    БлижайщиеДатыПоТоварам.СкладНазначения AS СкладНазначения
Into #Temp_NearestDate
from #Temp_ClosestDatesByGoods БлижайщиеДатыПоТоварам With (NOLOCK)
    LEFT OUTER JOIN Temp_CountOfGoods
	    ON 1 = 1
	LEFT OUTER JOIN #Temp_PickupDateShift СмещениеДатДоставки
	On СмещениеДатДоставки.Склад = БлижайщиеДатыПоТоварам.СкладНазначения
    And БлижайщиеДатыПоТоварам.БлижайшаяДата < DATEADD(DAY, 2, @P_DateTimePeriodBegin) -- дата меньше чем конец завтрашнего дня
Group by БлижайщиеДатыПоТоварам.СкладНазначения
HAVING COUNT(DISTINCT БлижайщиеДатыПоТоварам.НоменклатураСсылка) = MIN(Temp_CountOfGoods.CountOfGoods)
Order by DateAvailable ASC
OPTION (KEEP PLAN, KEEPFIXED PLAN);

WITH DateForGtinRegistration AS
(
	SELECT 
		MAX(T1.Дата) AS DateAvailable
	FROM
		(SELECT TOP {7} -- здесь ГП EPassКоличествоРабочихДнейДляРегистрацииШтрихкода
			ДанныеКалендаря._Fld14262 AS Дата
		FROM #Temp_NearestDate
		INNER JOIN _InfoRg14260 ДанныеКалендаря 
		ON #Temp_NearestDate.DateAvailable < ДанныеКалендаря._Fld14262
			AND ДанныеКалендаря._Fld14261RRef = 0x8265002522BD9FAE11E4C0CE607941B6 -- календарь = Республика Беларусь
			AND ДанныеКалендаря._Fld14264RRef IN (0xA826C921F976C5EE45F87E7C18D0A858, 0xAC0042F13CCF80E7466E4329C5762C35)--рабочий, предпразничный
			AND ДанныеКалендаря._Fld14262 <= @P_DateTimePeriodEnd) T1)
SELECT 
	#Temp_NearestDate.СкладНазначения,
	-- Если среди товаров есть маркируемые/потенциально маркируемые без зарегистрированного в EPass Gtin, то к дате доступности прибавляем количество рабочих дней из гп
	CASE
		WHEN #Temp_MarkedGoodsWithoutGtin.НоменклатураСсылка IS NULL
			OR DateForGtinRegistration.DateAvailable IS NULL 
			THEN #Temp_NearestDate.DateAvailable
		ELSE DATEADD(HOUR, 
			DATEPART(HOUR, #Temp_NearestDate.DateAvailable), 
			DateForGtinRegistration.DateAvailable) -- Добавляем количество часов из исходной даты доступности

	END AS DateAvailable 
INTO #Temp_DateAvailable
FROM #Temp_NearestDate
	LEFT JOIN #Temp_MarkedGoodsWithoutGtin ON 1 = 1
	LEFT JOIN DateForGtinRegistration ON 1 = 1
OPTION (KEEP PLAN, KEEPFIXED PLAN);
/*Тут закончился процесс оптимальной даты. Склад назначения нужен чтоб потом правильную ГП выбрать*/

/*Интервалы для ПВЗ*/
WITH Tdate(date, СкладНазначения) AS (
    /*Это получение списка дат интервалов после даты окончания расчета*/
    SELECT         
		CAST(CAST(#Temp_DateAvailable.DateAvailable  AS DATE) AS DATETIME), 		
		#Temp_DateAvailable.СкладНазначения
	From #Temp_DateAvailable
	Where #Temp_DateAvailable.СкладНазначения in (select СкладСсылка From #Temp_PickupPoints)
    UNION
    ALL
    SELECT 
        DateAdd(day, 1, Tdate.date),
		#Temp_DateAvailable.СкладНазначения
    FROM
        Tdate
		Inner Join #Temp_DateAvailable 
		ON Tdate.date < DateAdd(DAY, @P_DaysToShow, CAST(CAST(#Temp_DateAvailable.DateAvailable  AS DATE) AS DATETIME))
		AND Tdate.СкладНазначения = #Temp_DateAvailable.СкладНазначения
		AND #Temp_DateAvailable.СкладНазначения in (select СкладСсылка From #Temp_PickupPoints)
)
SELECT	
	CASE 
	WHEN 
		DATEADD(
			SECOND,
			CAST(
				DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27057, ПВЗГрафикРаботы._Fld23617)) AS NUMERIC(12)
			),
			date
		) < #Temp_DateAvailable.DateAvailable 
		then #Temp_DateAvailable.DateAvailable
	Else
		DATEADD(
			SECOND,
			CAST(
				DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27057, ПВЗГрафикРаботы._Fld23617)) AS NUMERIC(12)
			),
			date
		)
	End As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27058, ПВЗГрафикРаботы._Fld23618)) AS NUMERIC(12)
        ),
        date
    ) As ВремяОкончания
Into #Temp_AvailablePickUp
FROM
    #Temp_DateAvailable
		Inner Join Tdate On 
			#Temp_DateAvailable.СкладНазначения = Tdate.СкладНазначения
		Inner Join dbo._Reference226 Склады ON Склады._IDRRef = #Temp_DateAvailable.СкладНазначения
			Inner Join _Reference23612 On Склады._Fld23620RRef = _Reference23612._IDRRef
				Left Join _Reference23612_VT23613 As ПВЗГрафикРаботы 
				On _Reference23612._IDRRef = _Reference23612_IDRRef
				AND (case when @@DATEFIRST = 1 then DATEPART ( dw , Tdate.date ) when DATEPART ( dw , Tdate.date ) = 1 then 7 else DATEPART ( dw , Tdate.date ) -1 END) = ПВЗГрафикРаботы._Fld23615
		Left Join _Reference23612_VT27054 As ПВЗИзмененияГрафикаРаботы 
				On _Reference23612._IDRRef = ПВЗИзмененияГрафикаРаботы._Reference23612_IDRRef
				AND Tdate.date = ПВЗИзмененияГрафикаРаботы._Fld27056
		WHERE 
			case 
			when ПВЗИзмененияГрафикаРаботы._Reference23612_IDRRef is not null
				then ПВЗИзмененияГрафикаРаботы._Fld27059
			when ПВЗГрафикРаботы._Reference23612_IDRRef is not Null 
				then ПВЗГрафикРаботы._Fld25265 
			else 0 --не найдено ни графика ни изменения графика  
			end = 0x00  -- не выходной
		AND DATEADD(
			SECOND,
			CAST(
            DATEDIFF(SECOND, @P_EmptyDate, isNull(ПВЗИзмененияГрафикаРаботы._Fld27058, ПВЗГрафикРаботы._Fld23618)) AS NUMERIC(12)
			),
			Tdate.date) > #Temp_DateAvailable.DateAvailable
OPTION (KEEP PLAN, KEEPFIXED PLAN);
/*Конец интервалов для ПВЗ*/

/*Мощности доставки*/
SELECT
    CAST(
        SUM(
            CASE
                WHEN (МощностиДоставки._RecordKind = 0.0) THEN МощностиДоставки._Fld25107
                ELSE -(МощностиДоставки._Fld25107)
            END
        ) AS NUMERIC(16, 3)
    ) AS МассаОборот,
    CAST(
        SUM(
            CASE
                WHEN (МощностиДоставки._RecordKind = 0.0) THEN МощностиДоставки._Fld25108
                ELSE -(МощностиДоставки._Fld25108)
            END
        ) AS NUMERIC(16, 3)
    ) AS ОбъемОборот,
    CAST(
        SUM(
            CASE
                WHEN (МощностиДоставки._RecordKind = 0.0) THEN МощностиДоставки._Fld25201
                ELSE -(МощностиДоставки._Fld25201)
            END
        ) AS NUMERIC(16, 2)
    ) AS ВремяНаОбслуживаниеОборот,
	CAST(CAST(МощностиДоставки._Period  AS DATE) AS DATETIME) AS Дата
Into #Temp_DeliveryPower
FROM
    dbo._AccumRg25104 МощностиДоставки With (NOLOCK),
	#Temp_Size With (NOLOCK),
	#Temp_TimeService With (NOLOCK)
WHERE
    МощностиДоставки._Period >= @P_DateTimePeriodBegin
    AND МощностиДоставки._Period <= @P_DateTimePeriodEnd
	AND МощностиДоставки._Fld25105RRef IN (Select ЗонаДоставкиРодительСсылка From  #Temp_GeoData)
GROUP BY
	CAST(CAST(МощностиДоставки._Period  AS DATE) AS DATETIME),
	#Temp_Size.Вес,
	#Temp_Size.Объем,
	#Temp_TimeService.ВремяВыполнения
Having 
	SUM(
            CASE
                WHEN (МощностиДоставки._RecordKind = 0.0) THEN МощностиДоставки._Fld25107
                ELSE -(МощностиДоставки._Fld25107)
            END
        ) > #Temp_Size.Вес
	AND 
	SUM(
            CASE
                WHEN (МощностиДоставки._RecordKind = 0.0) THEN МощностиДоставки._Fld25108
                ELSE -(МощностиДоставки._Fld25108)
            END
        ) > #Temp_Size.Объем
	And 
	SUM(
            CASE
                WHEN (МощностиДоставки._RecordKind = 0.0) THEN МощностиДоставки._Fld25201
                ELSE -(МощностиДоставки._Fld25201)
            END
        ) > #Temp_TimeService.ВремяВыполнения	
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}',@P_DateTimePeriodEnd='{3}'),KEEP PLAN, KEEPFIXED PLAN);

/*Тут начинаются интервалы, которые рассчитанные*/
Select Distinct
	Case 
		When DATEPART(MINUTE,ГрафикПланирования._Fld23333) > 0 
		Then DATEADD(HOUR,1,ГрафикПланирования._Fld23333) 
		else ГрафикПланирования._Fld23333 
	End As ВремяВыезда,
	ГрафикПланирования._Fld23321 AS Дата,
	ГрафикПланирования._Fld23322RRef AS ГруппаПланирования
Into #Temp_CourierDepartureDates
From 
	dbo._InfoRg23320 AS ГрафикПланирования With (NOLOCK)
	INNER JOIN #Temp_PlanningGroups T2 With (NOLOCK) ON (ГрафикПланирования._Fld23322RRef = T2.ГруппаПланирования) 
Where ГрафикПланирования._Fld23321 BETWEEN @P_DateTimePeriodBegin AND @P_DateTimePeriodEnd 
	AND ГрафикПланирования._Fld23333 > @P_DateTimeNow
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}',@P_DateTimePeriodEnd='{3}',@P_DateTimeNow='{1}'),KEEP PLAN, KEEPFIXED PLAN);

SELECT
    T5._Period AS Период,
    T5._Fld25112RRef As ГруппаПланирования, 
	T5._Fld25111RRef As Геозона,
	T2.Основная AS Приоритет,
	T5._Fld25202 As ВремяНачалаНачальное,
	T5._Fld25203 As ВремяОкончанияНачальное,
    DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T5._Fld25202) AS NUMERIC(12)
        ),
        T5._Period
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, T5._Fld25203) AS NUMERIC(12)
        ),
        T5._Period
    ) AS ВремяОкончания,
    SUM(
                CASE
                    WHEN (T5._RecordKind = 0.0) THEN T5._Fld30385
                    ELSE -(T5._Fld30385)
                END
            ) AS КоличествоЗаказовЗаИнтервалВремениВВашеВремя,
	SUM(
                CASE
                    WHEN (T5._RecordKind = 0.0) THEN T5._Fld25113
                    ELSE -(T5._Fld25113)
                END
            ) AS КоличествоЗаказовЗаИнтервалВремени
into #Temp_IntervalsAll_old
FROM
    dbo._AccumRg25110 T5 With (NOLOCK)
	INNER JOIN #Temp_PlanningGroups T2 With (NOLOCK) ON (T5._Fld25112RRef = T2.ГруппаПланирования)
	AND T2.Склад IN (select СкладНазначения From #Temp_DateAvailable)
WHERE
    T5._Period >= @P_DateTimePeriodBegin --begin +2
    AND T5._Period <= @P_DateTimePeriodEnd --end
    AND T5._Fld25111RRef in (Select Геозона From #Temp_GeoData) 
	AND T5._Period IN (Select Дата From #Temp_DeliveryPower)
GROUP BY
    T5._Period,
    T5._Fld25112RRef,
    T5._Fld25111RRef,
    T5._Fld25202,
	T5._Fld25203,
	T2.Основная
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
        AND CASE WHEN @P_YourTimeDelivery = 1
			THEN CAST(
				SUM(
					CASE
						WHEN (T5._RecordKind = 0.0) THEN T5._Fld30385
						ELSE -(T5._Fld30385)
					END
				) AS NUMERIC(16, 0)
			) 
			ELSE 1.0
			END > 0.0
    )
OPTION (HASH GROUP, OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}',@P_DateTimePeriodEnd='{3}'), KEEP PLAN, KEEPFIXED PLAN);


Select Distinct
	ВременныеИнтервалы.Период AS Период,
	ВременныеИнтервалы.Геозона AS Геозона,
	ВременныеИнтервалы.ГруппаПланирования AS ГруппаПланирования,
	ВременныеИнтервалы.ВремяНачалаНачальное AS ВремяНачалаНачальное,
	ВременныеИнтервалы.ВремяОкончанияНачальное AS ВремяОкончанияНачальное,
	ВременныеИнтервалы.КоличествоЗаказовЗаИнтервалВремени AS КоличествоЗаказовЗаИнтервалВремени,
    ВременныеИнтервалы.КоличествоЗаказовЗаИнтервалВремениВВашеВремя AS КоличествоЗаказовЗаИнтервалВремениВВашеВремя,
	ВременныеИнтервалы.ВремяНачала AS ВремяНачала,
	ВременныеИнтервалы.ВремяОкончания AS ВремяОкончания,
	ВременныеИнтервалы.Приоритет
Into #Temp_IntervalsAll
From
	#Temp_IntervalsAll_old AS ВременныеИнтервалы
		Inner Join #Temp_CourierDepartureDates AS ВТ_ГрафикПланирования
		ON DATEPART(HOUR, ВТ_ГрафикПланирования.ВремяВыезда) <= DATEPART(HOUR, ВременныеИнтервалы.ВремяНачалаНачальное)
		AND ВременныеИнтервалы.ГруппаПланирования = ВТ_ГрафикПланирования.ГруппаПланирования
	    AND ВременныеИнтервалы.Период = ВТ_ГрафикПланирования.Дата
OPTION (KEEP PLAN, KEEPFIXED PLAN);

select
DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25129) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) AS ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) AS КоличествоЗаказовЗаИнтервалВремени, 
    Case when ГеоЗонаВременныеИнтервалы._Fld27342 = 0x01 then 1 else 0 End AS Стимулировать,
    Case when ГеоЗонаВременныеИнтервалы._Fld32163 = 0x01 then 1 else 0 End AS ЗагруженныйБудни,
	Case when ГеоЗонаВременныеИнтервалы._Fld32164 = 0x01 then 1 else 0 End AS ЗагруженныйВыходные,
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
    AND @P_YourTimeDelivery = 0

Group By 
	ГеоЗонаВременныеИнтервалы._Fld25128,
	ГеоЗонаВременныеИнтервалы._Fld25129,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет,
    ГеоЗонаВременныеИнтервалы._Fld27342,
    ГеоЗонаВременныеИнтервалы._Fld32163,
	ГеоЗонаВременныеИнтервалы._Fld32164
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
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25129) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) AS ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) AS КоличествоЗаказовЗаИнтервалВремени,
    Case when ГеоЗонаВременныеИнтервалы._Fld27342 = 0x01 then 1 else 0 End AS Стимулировать,
    Case when ГеоЗонаВременныеИнтервалы._Fld32163 = 0x01 then 1 else 0 End AS ЗагруженныйБудни,
	Case when ГеоЗонаВременныеИнтервалы._Fld32164 = 0x01 then 1 else 0 End AS ЗагруженныйВыходные,
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
    AND @P_YourTimeDelivery = 0

Group By 
	ГеоЗонаВременныеИнтервалы._Fld25128,
	ГеоЗонаВременныеИнтервалы._Fld25129,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет,
    ГеоЗонаВременныеИнтервалы._Fld27342,
    ГеоЗонаВременныеИнтервалы._Fld32163,
	ГеоЗонаВременныеИнтервалы._Fld32164
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
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25129) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) AS ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) AS КоличествоЗаказовЗаИнтервалВремени,
    Case when ГеоЗонаВременныеИнтервалы._Fld27342 = 0x01 then 1 else 0 End AS Стимулировать,
    Case when ГеоЗонаВременныеИнтервалы._Fld32163 = 0x01 then 1 else 0 End AS ЗагруженныйБудни,
	Case when ГеоЗонаВременныеИнтервалы._Fld32164 = 0x01 then 1 else 0 End AS ЗагруженныйВыходные,
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
	#Temp_IntervalsAll.Период BETWEEN DATEADD(DAY, 2, @P_DateTimePeriodBegin) 
    AND @P_DateTimePeriodEnd --begin +2
    AND @P_YourTimeDelivery = 0

Group By 
	ГеоЗонаВременныеИнтервалы._Fld25128,
	ГеоЗонаВременныеИнтервалы._Fld25129,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет,
    ГеоЗонаВременныеИнтервалы._Fld27342,
    ГеоЗонаВременныеИнтервалы._Fld32163,
	ГеоЗонаВременныеИнтервалы._Fld32164
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}',@P_DateTimePeriodEnd='{3}'), KEEP PLAN, KEEPFIXED PLAN);

Insert into #Temp_Intervals
Select
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30390) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30391) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) AS ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) AS КоличествоЗаказовЗаИнтервалВремени,
	0 AS Стимулировать,
    0 AS ЗагруженныйБудни,
	0 AS ЗагруженныйВыходные,
    #Temp_IntervalsAll.Период,
    #Temp_IntervalsAll.ГруппаПланирования,
    #Temp_IntervalsAll.Геозона,
    #Temp_IntervalsAll.Приоритет
From #Temp_IntervalsAll
	Inner Join _Reference114_VT30388 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And (
			(#Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld30391)
			Or (#Temp_IntervalsAll.ВремяОкончанияНачальное > ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяОкончанияНачальное <= ГеоЗонаВременныеИнтервалы._Fld30391)
			)

WHERE
	#Temp_IntervalsAll.Период BETWEEN DATEADD(DAY, 2, @P_DateTimePeriodBegin) AND @P_DateTimePeriodEnd --begin +2
    AND @P_YourTimeDelivery = 1

Group By 
	ГеоЗонаВременныеИнтервалы._Fld30390,
	ГеоЗонаВременныеИнтервалы._Fld30391,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}',@P_DateTimePeriodEnd='{3}'), KEEP PLAN, KEEPFIXED PLAN);

Insert into #Temp_Intervals
Select
DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30390) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30391) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) AS ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) AS КоличествоЗаказовЗаИнтервалВремени,
	0 AS Стимулировать,
    0 AS ЗагруженныйБудни,
	0 AS ЗагруженныйВыходные,
    #Temp_IntervalsAll.Период,
    #Temp_IntervalsAll.ГруппаПланирования,
    #Temp_IntervalsAll.Геозона,
    #Temp_IntervalsAll.Приоритет
from #Temp_IntervalsAll
	Inner Join _Reference114_VT30388 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And (
			(#Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld30391)
			Or (#Temp_IntervalsAll.ВремяОкончанияНачальное > ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяОкончанияНачальное <= ГеоЗонаВременныеИнтервалы._Fld30391)
			)
  INNER JOIN dbo._Reference23294 T4 With (NOLOCK) ON (#Temp_IntervalsAll.ГруппаПланирования = T4._IDRRef)
    AND (
        (@P_TimeNow < T4._Fld25140)
        OR (ГеоЗонаВременныеИнтервалы._Fld30390 >= T4._Fld25139)
    )
WHERE
    #Temp_IntervalsAll.Период = DATEADD(DAY, 1, @P_DateTimePeriodBegin)
    AND @P_YourTimeDelivery = 1

Group By 
	ГеоЗонаВременныеИнтервалы._Fld30390,
	ГеоЗонаВременныеИнтервалы._Fld30391,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}'), KEEP PLAN, KEEPFIXED PLAN);

Insert into #Temp_Intervals
Select
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30390) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) As ВремяНачала,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld30391) AS NUMERIC(12)
        ),
        #Temp_IntervalsAll.Период
    ) AS ВремяОкончания,
	Sum(#Temp_IntervalsAll.КоличествоЗаказовЗаИнтервалВремени) AS КоличествоЗаказовЗаИнтервалВремени, 
    0 AS Стимулировать,
    0 AS ЗагруженныйБудни,
	0 AS ЗагруженныйВыходные,
    #Temp_IntervalsAll.Период,
    #Temp_IntervalsAll.ГруппаПланирования,
    #Temp_IntervalsAll.Геозона,
    #Temp_IntervalsAll.Приоритет
From #Temp_IntervalsAll
	INNER JOIN _Reference114_VT30388 ГеоЗонаВременныеИнтервалы With (NOLOCK)
		On #Temp_IntervalsAll.Геозона = ГеоЗонаВременныеИнтервалы._Reference114_IDRRef
		And (
			(#Temp_IntervalsAll.ВремяНачалаНачальное >= ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяНачалаНачальное < ГеоЗонаВременныеИнтервалы._Fld30391)
			Or (#Temp_IntervalsAll.ВремяОкончанияНачальное > ГеоЗонаВременныеИнтервалы._Fld30390 And #Temp_IntervalsAll.ВремяОкончанияНачальное <= ГеоЗонаВременныеИнтервалы._Fld30391)
			)
	INNER JOIN dbo._Reference23294 T2 With (NOLOCK) 
		ON (#Temp_IntervalsAll.ГруппаПланирования = T2._IDRRef)
		AND (ГеоЗонаВременныеИнтервалы._Fld30390 >= T2._Fld30397)
		AND (NOT (((@P_TimeNow >= T2._Fld25138))))
WHERE
    #Temp_IntervalsAll.Период = @P_DateTimePeriodBegin
    AND @P_YourTimeDelivery = 1

Group By 
	ГеоЗонаВременныеИнтервалы._Fld30390,
	ГеоЗонаВременныеИнтервалы._Fld30391,
	#Temp_IntervalsAll.Период,
	#Temp_IntervalsAll.ГруппаПланирования,
	#Temp_IntervalsAll.Геозона,
	#Temp_IntervalsAll.Приоритет
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}'), KEEP PLAN, KEEPFIXED PLAN);

select Период, Max(Приоритет) AS Приоритет into #Temp_PlanningGroupPriority from #Temp_Intervals Group by Период;
/*Выше закончились рассчитанные интервалы*/

WITH T(date) AS (
    /*Это получение списка дат интервалов после даты окончания расчета*/
    SELECT
        Case When @P_DateTimePeriodEnd >= CAST(CAST(#Temp_DateAvailable.DateAvailable  AS DATE) AS DATETIME) Then
		DateAdd(day, 1,
		@P_DateTimePeriodEnd
		)
		else 
		CAST(CAST(#Temp_DateAvailable.DateAvailable  AS DATE) AS DATETIME) 
		End
	From #Temp_DateAvailable
    UNION
    ALL
    SELECT
        DateAdd(day, 1, T.date)
    FROM
        T
		Inner Join #Temp_DateAvailable 
		ON T.date < DateAdd(DAY, @P_DaysToShow, CAST(CAST(#Temp_DateAvailable.DateAvailable  AS DATE) AS DATETIME)) 
)
/*Тут мы выбираем даты из регистра*/
select 
	#Temp_Intervals.ВремяНачала As StartDate,
	#Temp_Intervals.ВремяОкончания As EndDate,
	SUM(
	#Temp_Intervals.КоличествоЗаказовЗаИнтервалВремени
	) 
	AS OrdersCount,
    #Temp_Intervals.Стимулировать As Bonus,
    #Temp_Intervals.Период,
	#Temp_Intervals.ЗагруженныйБудни,
	#Temp_Intervals.ЗагруженныйВыходные
Into #Temp_IntervalsWithOutShifting
From
#Temp_Intervals With (NOLOCK)
Inner Join #Temp_DateAvailable With (NOLOCK) 
    On #Temp_Intervals.ВремяНачала >= #Temp_DateAvailable.DateAvailable
Inner Join #Temp_TimeService With (NOLOCK) On 1=1
Inner Join #Temp_PlanningGroupPriority With (NOLOCK) ON #Temp_Intervals.Период = #Temp_PlanningGroupPriority.Период AND #Temp_Intervals.Приоритет = #Temp_PlanningGroupPriority.Приоритет
Inner Join #Temp_PlanningGroups With (NOLOCK) ON #Temp_Intervals.ГруппаПланирования = #Temp_PlanningGroups.ГруппаПланирования
Where #Temp_Intervals.Период >= DATEADD(DAY, @P_Credit, @P_DateTimePeriodBegin) -- для кредита возвращаем даты начиная со следующего дня 
Group By 
	#Temp_Intervals.ВремяНачала,
	#Temp_Intervals.ВремяОкончания,
	#Temp_Intervals.Период,
	#Temp_TimeService.ВремяВыполнения,
    #Temp_PlanningGroups.СреднееВремяНаПереезд,
    #Temp_Intervals.Стимулировать,
    #Temp_Intervals.Период,
	#Temp_Intervals.ЗагруженныйБудни,
	#Temp_Intervals.ЗагруженныйВыходные
Having SUM(#Temp_Intervals.КоличествоЗаказовЗаИнтервалВремени) > (#Temp_TimeService.ВремяВыполнения + #Temp_PlanningGroups.СреднееВремяНаПереезд)

Union
All
/*А тут мы выбираем даты где логисты еще не рассчитали*/
SELECT
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) AS NUMERIC(12)
        ),
        date
    ) As StartDate,
	DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25129) AS NUMERIC(12)
        ),
        date
    ) As EndDate,
	0 AS OrdersCount,
    Case when ГеоЗонаВременныеИнтервалы._Fld27342 = 0x01 then 1 else 0 End AS Bonus,
    T.date,
	Case when ГеоЗонаВременныеИнтервалы._Fld32163 = 0x01 then 1 else 0 End,
	Case when ГеоЗонаВременныеИнтервалы._Fld32164 = 0x01 then 1 else 0 End
FROM
    T 
	Inner Join _Reference114_VT25126 AS ГеоЗонаВременныеИнтервалы  With (NOLOCK) 
    On ГеоЗонаВременныеИнтервалы._Reference114_IDRRef In (Select Геозона From #Temp_GeoData)
        And @P_YourTimeDelivery = 0
	Inner Join #Temp_DateAvailable On DATEADD(
        SECOND,
        CAST(
            DATEDIFF(SECOND, @P_EmptyDate, ГеоЗонаВременныеИнтервалы._Fld25128) AS NUMERIC(12)
        ),
        date
    ) >= #Temp_DateAvailable.DateAvailable
UNION ALL
Select 
	#Temp_AvailablePickUp.ВремяНачала As StartDate,
	#Temp_AvailablePickUp.ВремяОкончания As EndDate,
	0 As OrdersCount,
    0 As Bonus,
    @P_EmptyDate,
	0,
	0
From #Temp_AvailablePickUp
OPTION (OPTIMIZE FOR (@P_DateTimePeriodBegin='{2}',@P_DateTimePeriodEnd='{3}'), KEEP PLAN, KEEPFIXED PLAN);

Select 
	IntervalsWithOutShifting.StartDate
INTO #Temp_UnavailableDates
From #Temp_Goods as TempGoods
inner join dbo._InfoRg28348 as ПрослеживаемыеТоварныеКатегории WITH(NOLOCK)
		on 1 = @P_ApplyShifting -- это будет значение ГП ПрименятьСмещениеДоступностиПрослеживаемыхМаркируемыхТоваров
			and ПрослеживаемыеТоварныеКатегории._Fld28349RRef = TempGoods.ТоварнаяКатегорияСсылка
			and @P_DateTimeNow <= DateAdd(DAY, @P_DaysToShift, ПрослеживаемыеТоварныеКатегории._Period)  -- количество дней будет из ГП
inner join #Temp_IntervalsWithOutShifting as IntervalsWithOutShifting
		on IntervalsWithOutShifting.StartDate between ПрослеживаемыеТоварныеКатегории._period AND DateAdd(DAY, @P_DaysToShift, ПрослеживаемыеТоварныеКатегории._Period)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

select 
	IntervalsWithOutShifting.Период,
	IntervalsWithOutShifting.StartDate,
	IntervalsWithOutShifting.EndDate,
	IntervalsWithOutShifting.Bonus,
	Case When ДанныеКалендаря._Fld14264RRef in (0xA826C921F976C5EE45F87E7C18D0A858, 0xAC0042F13CCF80E7466E4329C5762C35) --рабочий, предпразничный
		Then IntervalsWithOutShifting.ЗагруженныйБудни
		Else IntervalsWithOutShifting.ЗагруженныйВыходные
	End As ЗагруженныйИнтервал,
    DATEDIFF(DAY, @P_DateTimePeriodBegin, IntervalsWithOutShifting.Период) + 1 As НомерДняДоставки,
	IsNull(ГеоЗоны.ЦелевойДень, 0) As ЦелевойДень,
    IsNull(ГеоЗоны.ВремяСтопаНаЦелевойДень, @P_EmptyDate) As ВремяСтопаНаЦелевойДень,
    IsNull(ГеоЗоны.ВремяНачалаДеньПослеЦелевого, @P_EmptyDate) As ВремяНачалаДеньПослеЦелевого,
	IsNull(ГеоЗоны.ПланируемыеИнтервалыС, 0) As ПланируемыеИнтервалыС,
	IsNull(ГеоЗоны.ПланируемыеИнтервалыПо, 0) ПланируемыеИнтервалыПо
Into #Temp_AvailableIntervals
from #Temp_IntervalsWithOutShifting as IntervalsWithOutShifting  
	left join #Temp_UnavailableDates as UnavailableDates 
		on IntervalsWithOutShifting.StartDate = UnavailableDates.StartDate
	left join _InfoRg14260 ДанныеКалендаря 
		On IntervalsWithOutShifting.Период = ДанныеКалендаря._Fld14262
		And ДанныеКалендаря._Fld14261RRef = 0x8265002522BD9FAE11E4C0CE607941B6 -- календарь = Республика Беларусь
	left join #Temp_GeoData ГеоЗоны
		On 1 = 1
where 
	UnavailableDates.StartDate is NULL
OPTION (KEEP PLAN, KEEPFIXED PLAN);

SELECT
	T1.Период As Период,
	T1.МассаПриход As МассаПриход,
	T1.МассаРасход As МассаРасход,
	T1.ОбъемПриход As ОбъемПриход,
	T1.ОбъемРасход As ОбъемРасход,
	T1.ВремяНаОбслуживаниеПриход As ВремяНаОбслуживаниеПриход,
	T1.ВремяНаОбслуживаниеРасход As ВремяНаОбслуживаниеРасход
Into #Temp_DeliveryPowerUsage
FROM (SELECT
	DATETIME2FROMPARTS(DATEPART(YEAR,МощностиДоставки._Period),DATEPART(MONTH,МощностиДоставки._Period),DATEPART(DAY,МощностиДоставки._Period),0,0,0,0,0) AS Период,
	ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN МощностиДоставки._Fld25108 ELSE 0.0 END) AS NUMERIC(16, 3)) AS NUMERIC(16, 3)),0.0) AS ОбъемПриход,
	ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN 0.0 ELSE МощностиДоставки._Fld25108 END) AS NUMERIC(16, 3)) AS NUMERIC(16, 3)),0.0) AS ОбъемРасход,
	ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN МощностиДоставки._Fld25107 ELSE 0.0 END) AS NUMERIC(16, 3)) AS NUMERIC(16, 3)),0.0) AS МассаПриход,
	ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN 0.0 ELSE МощностиДоставки._Fld25107 END) AS NUMERIC(16, 3)) AS NUMERIC(16, 3)),0.0) AS МассаРасход,
	ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN МощностиДоставки._Fld25201 ELSE 0.0 END) AS NUMERIC(16, 2)) AS NUMERIC(16, 2)),0.0) AS ВремяНаОбслуживаниеПриход,
	ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN 0.0 ELSE МощностиДоставки._Fld25201 END) AS NUMERIC(16, 2)) AS NUMERIC(16, 2)),0.0) AS ВремяНаОбслуживаниеРасход
	FROM dbo._AccumRg25104 МощностиДоставки WITH(NOLOCK)
	WHERE МощностиДоставки._Period >= @P_DateTimePeriodBegin 
	AND МощностиДоставки._Period <= DateAdd(Day, 1, @P_DateTimePeriodEnd) 
	AND МощностиДоставки._Active = 0x01 
	AND (МощностиДоставки._Fld25105RRef IN
		(SELECT
			Геодата.ЗонаДоставкиРодительСсылка AS ЗонаДоставкиРодительСсылка
		FROM #Temp_GeoData Геодата WITH(NOLOCK)
		WHERE Геодата.УчетИспользованияМощностей = 0x01
		)
	)
GROUP BY DATETIME2FROMPARTS(DATEPART(YEAR,МощностиДоставки._Period),DATEPART(MONTH,МощностиДоставки._Period),DATEPART(DAY,МощностиДоставки._Period),0,0,0,0,0)
HAVING (ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN МощностиДоставки._Fld25108 ELSE 0.0 END) AS NUMERIC(16, 3)) AS NUMERIC(16, 3)),0.0)) <> 0.0 
OR (ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN 0.0 ELSE МощностиДоставки._Fld25108 END) AS NUMERIC(16, 3)) AS NUMERIC(16, 3)),0.0)) <> 0.0 
OR (ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN МощностиДоставки._Fld25107 ELSE 0.0 END) AS NUMERIC(16, 3)) AS NUMERIC(16, 3)),0.0)) <> 0.0 
OR (ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN 0.0 ELSE МощностиДоставки._Fld25107 END) AS NUMERIC(16, 3)) AS NUMERIC(16, 3)),0.0)) <> 0.0 
OR (ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN МощностиДоставки._Fld25201 ELSE 0.0 END) AS NUMERIC(16, 2)) AS NUMERIC(16, 2)),0.0)) <> 0.0 
OR (ISNULL(CAST(CAST(SUM(CASE WHEN МощностиДоставки._RecordKind = 0.0 THEN 0.0 ELSE МощностиДоставки._Fld25201 END) AS NUMERIC(16, 2)) AS NUMERIC(16, 2)),0.0)) <> 0.0) T1
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select
	Мощности.Период,
	Case When Мощности.МассаПриход = 0
		Then 0
		Else Мощности.МассаРасход / Мощности.МассаПриход * 100 
	End As ПроцентИспользования
Into #Temp_DeliveryPowerUsagePercent
From #Temp_DeliveryPowerUsage Мощности WITH(NOLOCK)

Union All

Select
	Мощности.Период,
	Case When Мощности.ОбъемПриход = 0
		Then 0
		Else Мощности.ОбъемРасход / Мощности.ОбъемПриход * 100 
	End
From #Temp_DeliveryPowerUsage Мощности WITH(NOLOCK)

Union All

Select
	Мощности.Период,
	Case When Мощности.ВремяНаОбслуживаниеПриход = 0
		Then 0
		Else Мощности.ВремяНаОбслуживаниеРасход / Мощности.ВремяНаОбслуживаниеПриход * 100 
	End
From #Temp_DeliveryPowerUsage Мощности WITH(NOLOCK)
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select
	Мощности.Период
Into #Temp_DeliveryPowerLoadedDates
From #Temp_DeliveryPowerUsagePercent Мощности WITH(NOLOCK)
Group by 
	Мощности.Период
Having 
	Max(Мощности.ПроцентИспользования) >= @P_LoadedIntervalsUsagePercent
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select Top 1
	ДоступныеИнтервалы.Период
Into #Temp_FirstDate
From #Temp_AvailableIntervals ДоступныеИнтервалы	
Order By ДоступныеИнтервалы.Период
OPTION (KEEP PLAN, KEEPFIXED PLAN);

Select
	ДоступныеИнтервалы.StartDate,
	ДоступныеИнтервалы.EndDate,
	ДоступныеИнтервалы.Bonus,
	Case When ДоступныеИнтервалы.ЦелевойДень > 0
		And ДоступныеИнтервалы.НомерДняДоставки = ДоступныеИнтервалы.ЦелевойДень -- целевой день доставки
		And ДоступныеИнтервалы.ВремяСтопаНаЦелевойДень > @P_TimeNow
			Then 'nearest'
		When ДоступныеИнтервалы.ЦелевойДень > 0
		And ДоступныеИнтервалы.НомерДняДоставки = ДоступныеИнтервалы.ЦелевойДень + 1 -- день доставки после целевого
		And ДоступныеИнтервалы.ВремяНачалаДеньПослеЦелевого < @P_TimeNow
			Then 'nearest'
		When ДоступныеИнтервалы.ЗагруженныйИнтервал = 1
		And DATEDIFF(DAY, ПерваяДата.Период, ДоступныеИнтервалы.Период) + 1 <= @LoadedIntervalsDays 
			Then 'loaded'
        When ЗагруженныеДаты.Период is not null
		And DATEDIFF(DAY, ПерваяДата.Период, ДоступныеИнтервалы.Период) + 1 <= @LoadedIntervalsDays 
			Then 'loaded'
		When DATEDIFF(DAY, ПерваяДата.Период, ДоступныеИнтервалы.Период) + 1 >= ДоступныеИнтервалы.ПланируемыеИнтервалыС
			And DATEDIFF(DAY, ПерваяДата.Период, ДоступныеИнтервалы.Период) + 1 <= ДоступныеИнтервалы.ПланируемыеИнтервалыПо
			Then 'planned'
		Else 
			'basic'
	End As IntervalType	
From #Temp_AvailableIntervals ДоступныеИнтервалы WITH(NOLOCK)
	Inner Join #Temp_FirstDate ПерваяДата WITH(NOLOCK)
		On 1 = 1
	Left Outer Join #Temp_DeliveryPowerLoadedDates ЗагруженныеДаты WITH(NOLOCK)
	On ДоступныеИнтервалы.Период = ЗагруженныеДаты.Период
Order by StartDate
OPTION (KEEP PLAN, KEEPFIXED PLAN);
";
    }
}
