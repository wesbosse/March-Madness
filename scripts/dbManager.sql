SELECT * 
FROM Seeds 
FULL OUTER JOIN Teams ON Seeds.TeamID=Teams.TeamID;

Select *
FROM TeamSpellings
LEFT JOIN Scraped ON TeamSpellings.Name = Scraped.Name;

SELECT * 
FROM NCAATourneyCompactResults as tr
FULL OUTER Teams ON tr.TeamID=Teams.TeamID;


