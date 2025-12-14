export function markdownToHtml(md) {

  md = md.replace(/\r\n?/g, "");

  md = md.replace(/</g, "&lt;").replace(/>/g, "&gt;");


  md = md.replace(/```([\s\S]*?)```/g, function(_, code) {
    return `<pre><code>${code.trim()}</code></pre>`;
  });


  md = md.replace(/`([^`]+)`/g, "<code>$1</code>");


  md = md.replace(/^###### (.*)$/gm, "<h6>$1</h6>");
  md = md.replace(/^##### (.*)$/gm, "<h5>$1</h5>");
  md = md.replace(/^#### (.*)$/gm, "<h4>$1</h4>");
  md = md.replace(/^### (.*)$/gm, "<h3>$1</h3>");
  md = md.replace(/^## (.*)$/gm, "<h2>$1</h2>");
  md = md.replace(/^# (.*)$/gm, "<h1>$1</h1>");


  md = md.replace(/\*\*\*(.*?)\*\*\*/g, "<strong><em>$1</em></strong>");
  md = md.replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>");
  md = md.replace(/\*(.*?)\*/g, "<em>$1</em>");


  md = md.replace(/~~(.*?)~~/g, "<del>$1</del>");


  md = md.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, "<img src=\"$2\" alt=\"$1\">");


  md = md.replace(/\[([^\]]+)\]\(([^)]+)\)/g, "<a href=\"$2\">$1</a>");

  md = md.replace(/^> (.*)$/gm, "<blockquote>$1</blockquote>");

  //md = md.replace(/^(?:\s*[-*] .*(?:\n|$))+?/gm, function(list) {
  //  const items = list
  //    .trim()
  //    .split(/\n/)
  //    .map(line => line.replace(/^[-*] (.*)/, "<li>$1</li>"))
  //    .join("");
  //  return `<ul>${items}</ul>`;
  //});


  //md = md.replace(/^(?:\s*\d+\. .*(?:\n|$))+?/gm, function(list) {
  //  const items = list
  //    .trim()
  //    .split(/\n/)
  //    .map(line => line.replace(/^\d+\. (.*)/, "<li>$1</li>"))
  //    .join("");
  //  return `<ol>${items}</ol>`;
  //});


  //md = md.replace(/^(?!<(h\d|ul|ol|li|blockquote|pre|img|code))(.+)$/gm,
  //                "<p>$1</p>");

  return markdownToHTMLTable(md);
}


export function markdownToHTMLTable(md){

    const re = /((?:\|.*\|(?:\n|$)){2,})/g
    md = md.replace(re,
      function(tableBlock) {
        const lines = tableBlock.trim().split("\n");

        if (lines.length < 2) return tableBlock; // not a table

        const headerLine = lines[0];
        const separatorLine = lines[1];

        // Validate separator row (--- | :---:)
        if (!/^\s*\|?(.+\|)+\s*$/.test(separatorLine)) return tableBlock;

        const headers = headerLine
          .trim()
          .split("|")
          .filter(Boolean)
          .map(h => `<th>${h.trim()}</th>`)
          .join("");

        const rows = lines.slice(2).map(row => {
          const cells = row
            .trim()
            .split("|")
            .filter(Boolean)
            .map(c => `<td>${c.trim()}</td>`)
            .join("");
          return `<tr>${cells}</tr>`;
        }).join("");

        return `
        <table>
          <thead>
            <tr>${headers}</tr>
          </thead>
          <tbody>
            ${rows}
          </tbody>
        </table>
        `;
      }
    );

    return md;

}