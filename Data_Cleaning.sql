-- Data Cleaning

SELECT * FROM world_layoffs.layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data and Fix errors
-- 3. Null values or Blank values
-- 4. Remove any Columns and Rows that are not neccessary 


CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

SELECT * FROM world_layoffs.layoffs_staging;





-- 1. Remove Duplicates

SELECT *, -- date is in backtick because it is a keyword in mysql
	ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,
            percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- Identify the duplicates
SELECT *
FROM (
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Cazoo'
;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO world_layoffs.layoffs_staging2 
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
FROM 
	world_layoffs.layoffs_staging;

-- deleting the duplicates
-- DELETE FROM world_layoffs.layoffs_staging2
-- WHERE row_num >= 2;

SELECT * FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

SELECT * FROM world_layoffs.layoffs_staging2;




-- 2. Standardizing data

SELECT company, TRIM(company)
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2 
SET company = TRIM(company);

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2 
WHERE industry LIKE 'Crypto%';

UPDATE world_layoffs.layoffs_staging2 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

SELECT DISTINCT country,TRIM(TRAILING '.' FROM country)
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE world_layoffs.layoffs_staging2 
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
STR_TO_DATE(`date` , '%m/%d/%Y')
FROM world_layoffs.layoffs_staging2;

UPDATE World_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date` , '%m/%d/%Y');

SELECT `date`
FROM world_layoffs.layoffs_staging2;

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';
-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. 
-- We can write a query that if there is another row with the same company name, it will update it to the non-null industry values

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
	ON t1.company = t2.company
    SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;