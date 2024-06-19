export function formatDate(dateStr: string) {
  const dateObj = new Date(dateStr);

  // Get the day, month, and year
  const day = dateObj.getDate();
  const month = dateObj.getMonth() + 1; // Months are zero-based in JavaScript
  const year = dateObj.getFullYear().toString(); // Get the last two digits of the year

  // Format the date as desired
  const formattedDate = `${month}/${day}/${year}`;
  return formattedDate;
}
