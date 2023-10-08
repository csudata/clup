window.onload = function(){
	const tdDoms = document.querySelectorAll("td")
	tdDoms.forEach((item,index)=>{
		if(item.innerText){
			let boxWidth = Math.floor(item.getBoundingClientRect().width);
			// +40原因时左右各有20px的margin值
			let textWidth = Math.floor(getElementWidthWithoutAppending(item) + 40);
			if(textWidth>=boxWidth){
				item.setAttribute("title",item.innerText)
			}
		}
		
	})
	function getElementWidthWithoutAppending(element) {
		const range = new Range();
		range.selectNodeContents(element);
		const width = range.getBoundingClientRect().width;
		return width;
	}
}