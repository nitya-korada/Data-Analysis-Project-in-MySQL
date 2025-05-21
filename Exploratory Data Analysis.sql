-- ============================================
-- SECTION 1: View Raw Data
-- ============================================

-- View all data from the staging table
SELECT * 
FROM layoffs_staging2;

-- ============================================
-- SECTION 2: Summary Statistics
-- ============================================

-- Get the maximum number of total layoffs and highest percentage of layoffs
SELECT MAX(total_laid_off) AS max_total_laid_off, 
       MAX(percentage_laid_off) AS max_percentage_laid_off
FROM layoffs_staging2;

-- ============================================
-- SECTION 3: Companies with 100% Layoffs
-- ============================================

-- List companies with 100% layoffs, ordered by number laid off
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- List companies with 100% layoffs, ordered by funds raised
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- ============================================
-- SECTION 4: Monthly Layoff Trends
-- ============================================

-- Total layoffs per calendar month (aggregated by month number)
SELECT MONTH(`date`) AS layoff_month, 
       SUM(total_laid_off) AS total_monthly_layoffs
FROM layoffs_staging2
GROUP BY MONTH(`date`)
ORDER BY total_monthly_layoffs DESC;

-- Get the range of dates in the dataset
SELECT MIN(`date`) AS earliest_date, 
       MAX(`date`) AS latest_date
FROM layoffs_staging2;

-- ============================================
-- SECTION 5: Layoffs by Company Stage
-- ============================================

-- Total layoffs by funding stage of companies
SELECT Stage, 
       SUM(total_laid_off) AS total_stage_layoffs
FROM layoffs_staging2
GROUP BY Stage
ORDER BY total_stage_layoffs DESC;

-- ============================================
-- SECTION 6: Monthly Rolling Total Layoffs
-- ============================================

-- Simple monthly total layoffs by extracted month
SELECT SUBSTRING(`date`, 6, 2) AS month, 
       SUM(total_laid_off) AS monthly_layoffs
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 6, 2) IS NOT NULL
GROUP BY month
ORDER BY month ASC;

-- Cumulative rolling layoffs by YYYY-MM month
WITH Rolling_total AS (
    SELECT SUBSTRING(`date`, 1, 7) AS month, 
           SUM(total_laid_off) AS total_off
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY month
)
SELECT month, 
       total_off, 
       SUM(total_off) OVER(ORDER BY month) AS rolling_total
FROM Rolling_total;

-- ============================================
-- SECTION 7: Company-wise Layoff Totals
-- ============================================

-- List of companies (sorting column missing; could add SUM if desired)
SELECT company
FROM layoffs_staging2
GROUP BY company
ORDER BY company;

-- ============================================
-- SECTION 8: Yearly Layoffs by Company
-- ============================================

-- Total layoffs grouped by company and year
SELECT Company, 
       YEAR(`date`) AS year, 
       SUM(total_laid_off) AS yearly_total
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY yearly_total DESC;

-- ============================================
-- SECTION 9: Top 5 Companies by Layoffs Per Year
-- ============================================

-- CTE: Get total layoffs per company per year
WITH company_year (company, years, total_laid_off) AS (
    SELECT company, 
           YEAR(`date`) AS years, 
           SUM(total_laid_off)
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
),

-- CTE: Rank companies per year by layoffs using DENSE_RANK
Company_Year_Rank AS (
    SELECT *, 
           DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
    FROM company_year
    WHERE years IS NOT NULL
)

-- Final selection: Top 5 companies by layoffs for each year
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5;
