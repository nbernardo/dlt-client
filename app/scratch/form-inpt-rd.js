const extremRe = /[ \r \< \$ \( \) \- \s A-Za-z0-9 \/\:\;\* \{ \} \[ \] \. \, \ç\à\á\ã\â\è\é\ê\ẽ\í\ì\î\ĩ\ó\ò\ô\õ\ú\ù\û\ũ \= \"]{0,}/.source;
const matchValueBind = /\(value\)\=\"[\w.{}]*\"\s?/.source, matchClose = /[\s]{0,}>/.source;
const matchForEachRE = '(forEach)=\"', mtchValue = '(value)="', matchChange = '(change)="';
const valueBindRE = new RegExp(extremRe + matchValueBind + extremRe + matchClose , "gi");

const dd = `
<div class="dynamic-_cmp08851962443451677Bucket">
   
   <div class="title-box">
      
      <div><i class="fab fa-bitbucket "></i>@label</div>
      
      <div class="statusicon"></div>
      
   </div>
   
   <div class="box">
      
      <div>@bucketUrl</div>
      
      <form id="fId_formRef" onsubmit="return false;">
         
         <p>Enter Bucket url:</p>
         <input type="text" (required)="true" (value)="bucketUrl" placeholder="Basket/Folder URL">
         <p>File pattern:</p>
         <input type="text" (required)="true" (value)="filePattern" placeholder="Ex: transactions*.csv">
      </form>
      
   </div>
   
</div>
`;

dd.replace(valueBindRE, (mt) => {

    console.log(mt);
    
})