-- ======================================
-- SECTION 1: Removing Duplicates
-- ======================================

-- View the original layoffs table
SELECT *
FROM layoffs;

-- Create a staging table with the same structure as the original table
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Verify the structure of the staging table
SELECT *
FROM layoffs_staging;

-- Insert all records from original table into staging table
INSERT layoffs_staging SELECT * FROM layoffs;

-- Identify duplicate records by assigning row numbers partitioned by all relevant columns
SELECT *,
row_number() over(partition by company, location, industry, total_laid_off, 
percentage_laid_off, 'date', country, funds_raised_millions) as row_num
FROM layoffs_staging;

-- Use CTE to select duplicates and assign row numbers
WITH duplicate_cte AS (
  SELECT *,
  row_number() over(partition by company, location, industry, total_laid_off, 
  percentage_laid_off, 'date', country, funds_raised_millions) as row_num
  FROM layoffs_staging
)
-- Attempting to delete duplicates directly from a CTE (Note: This will error out in some SQL dialects like MySQL)
DELETE
FROM duplicate_cte 
WHERE row_num > '1';

-- Create a second staging table with an additional row_num column
CREATE TABLE layoffs_staging2 (
  company text,
  location text,
  industry text,
  total_laid_off int DEFAULT NULL,
  percentage_laid_off text,
  date text,
  stage text,
  country text,
  funds_raised_millions int DEFAULT NULL,
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- View structure of new staging table
SELECT *
FROM layoffs_staging2;

-- Insert data into new staging table along with generated row numbers
INSERT INTO layoffs_staging2
SELECT *,
row_number() over(partition by company, location, industry, total_laid_off, 
percentage_laid_off, 'date', country, funds_raised_millions) as row_num
FROM layoffs_staging;

-- Delete rows that are duplicates (i.e., have row_num > 1)
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Confirm that duplicates are removed
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;


-- ======================================
-- SECTION 2: Standardizing Format
-- ======================================

-- View and trim whitespace from company names
SELECT company, (TRIM(company))
FROM layoffs_staging2;

-- Apply trimming to company names
UPDATE layoffs_staging2
SET company = Trim(company);

-- View unique values in Industry column
SELECT DISTINCT(Industry)
FROM layoffs_staging2
ORDER BY 1;

-- Find variations of 'Crypto' industry name
SELECT *
FROM layoffs_staging2
WHERE Industry LIKE 'Crypto%';

-- Standardize all Crypto industry variations to 'Crypto'
UPDATE layoffs_staging2
SET Industry = 'Crypto'
WHERE Industry LIKE 'Crypto%';

-- View distinct country values
SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

-- Trim trailing periods from country names (e.g., 'United States.')
SELECT DISTINCT TRIM(TRAILING '.' FROM Country)
FROM layoffs_staging2
ORDER BY 1;

-- Apply trimming fix to affected country names
UPDATE layoffs_staging2
SET Country = TRIM(TRAILING '.' FROM Country)
WHERE Country LIKE 'Unites States%';

-- Verify country values
SELECT *
FROM layoffs_staging2
WHERE Country = 'United States';

-- Convert string dates to proper date format
SELECT `date`,
STR_TO_DATE (`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Apply date conversion
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y');

-- Confirm all updates
SELECT * 
FROM layoffs_staging2;

-- Change column type of `date` to proper DATE type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- ======================================
-- SECTION 3: Handling NULL or Blank Values
-- ======================================

-- Find records where both total_laid_off and percentage_laid_off are NULL
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Find records where industry is NULL or blank
SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Check records for a specific company
SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Replace 'NOT NULL' literal strings in Industry column with actual NULL
UPDATE layoffs_staging2
SET Industry = NULL
WHERE Industry = 'NOT NULL';

-- Attempt to fill in missing industry values using non-null values from the same company
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.Company = t2.Company
WHERE t1.Industry IS NULL
AND t2.Industry IS NOT NULL;

-- Update NULL industries using matched values from same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.Company = t2.Company
SET t1.Industry = t2.Industry
WHERE t1.Industry IS NULL
AND t2.Industry IS NOT NULL;

-- Delete records where both layoff metrics are NULL (i.e., not useful data)
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- ======================================
-- SECTION 4: Remove Unnecessary Columns
-- ======================================

-- View cleaned dataset
SELECT *
FROM layoffs_staging2;

-- Remove the row_num column since it was used only for de-duplication
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
