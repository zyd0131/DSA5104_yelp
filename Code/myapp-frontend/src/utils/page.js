export function changePage(data){
    const res = []
    let each = []
    for(let i = 0; i < data.length; i++) {
        each.push({...data[i]})
        if(i % 10 === 9) {
            res.push(each)
            each = []
        }
    }
    if(each.length) {
        res.push(each)
    }
    return res
}