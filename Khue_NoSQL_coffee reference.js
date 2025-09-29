use "AN6004_IA"

//1. How many product categories are there? For each product category, show the number of records.

db.baristacoffeesalestbl.aggregate([
    {
        $group: {
            _id: "$product_category",
            records: {$sum: 1}
        }
    }
])
//the _id is compulsary for $group function

//2. What is the average caffeine per beverage type (coffee/tea/energy drink)?

db.caffeine_intake_tracker.find()

db.caffeine_intake_tracker.aggregate([
    {
        $project: {
            beverage:{
                $switch: {
                  branches: [
                     { case:{$eq: ["$beverage_tea","True"]}, then: "tea" },
                     { case:{$eq: ["$beverage_coffee","True"]}, then: "coffee"},
                     { case:{$eq: ["$beverage_energy_drink","True"]}, then: "energy drink"}
                    ],
                    default: "none" // not found mark as none
                }
            },
            caffeine_mg: 1 // include the caffeeine_mg field in this selection
        }
    }, //the first stage is to select the average caffeine_mg and and new field of type of beverage drawing information from the original true false column. 
    {$match: {beverage: {$ne:"none"}}}, //to exclude potential person not consuming any of those beverage marked as none
    {$group: {_id: "$beverage", avg_caffeine: {$avg: "$caffeine_mg"}, count: {$sum : 1}} // groupby calculation, in noSQL count can use sum: 1 to add one for every entry add for the groupby
    },
    {$sort: {avg_caffeine: -1}}
]);

//this question is an example of using the agrregation through several stage {select the field} -> {exclude none value} -> {groupby calculation} -> {field sorting}
//I can understand the narration of this function but it's very easy to miss the correct syntax for the three type of bracket in noSQL

//3. How does sleep impact rate vary by time of day (morning/afternoon/evening)?

db.caffeine_intake_tracker.aggregate([
    {
        $project: {
            time_of_day:{
                $switch: {
                  branches: [
                     { case:{$eq: ["$time_of_day_morning","True"]}, then: "morning" },
                     { case:{$eq: ["$time_of_day_afternoon","True"]}, then: "afternoon"},
                     { case:{$eq: ["$time_of_day_evening","True"]}, then: "evening"}
                    ],
                    default: "unknown" // not found mark as none
                }
            },
            sleep_impacted: 1
        }
    }, //the first stage is to select the sleep_impacted and and new field of type of time consuming drawing information from the original true false column. 
    {$match: {time_of_day: {$ne:"unknown"}}},
    {$group: {
        _id: "$time_of_day",
        impacted_rate: {$avg: {$toDouble: "$sleep_impacted"}},
        count: {$sum : 1}}
    },
    {$sort: {impacted_rate: -1}}
]);

//4. Bucket caffeine into Low/Med/High and compare average sleep quality

db.caffeine_intake_tracker.aggregate([
    {
        $bucket: {
              groupBy: "$caffeine_mg",
              boundaries: [ 0, 0.25, 0.5, 1.01],// Low: 0-0.25, Medium: 0.25-0.5, High: 0.5-1
              default: "unknown",
              output: {
                avg_sleep_quality: {$avg: "$sleep_quality"},
                avg_focus: {$avg: "$focus_level"},
                n: { $sum: 1 }
              }
            }
    },
    {
        $addFields: {
            caffeine_band:{
                $switch: {
                  branches: [
                     { case: {$eq: ["$_id", 0]}, then: "Low" },
                     { case: {$eq: ["$_id", 0.25]}, then: "Medium"},
                     { case: {$eq: ["$_id", 0.5]}, then: "High"}
                  ],
                  default: "unknown"
                }
            }
        }
    },
    {$project: {_id: 0}},
    {$sort: {caffeine_bank:1}}
]);
    
// I initially want to change $project for the $addFields but when I test it will only return the name of the new column not other calcuated in stgae 1 so have to use addfiel.
// I think i can still use project but the syntax might be longer as have to list down all the field.

//5. What is the total revenue and order count?

db.coffeesales.aggregate([
    {
        $group: { _id: null, count: {$sum: 1}, revenue: {$sum: {$toDouble: "$money"}}}
    },
    {$project: {_id: 0}}
])

//I just combine the toDouble function to the group function

//6. Which drink is most cash-heavy? (cash share by drink)

db.coffeesales.aggregate([
    {$addFields: {is_cash: {$eq: ["$cash_type":"cash"]},money_sum: {$toDouble: "$money"}}}, //return the boolean True/False for is cash type
    {$group: { 
        _id: "$coffee_name",
        cash_orders: {$sum: {$cond: [ "$is_cash", 1 , 0]}}, //if it's cash add it to the calculation
        total_orders: {$sum: 1},
        cash_rev: {$sum: {$cond: [ "$is_cash", "$money_sum" , 0]}}, //if it's cash add it to the calculation
        total_rev: {$sum: "$money_sum"}
    }}, //create the summary of total vs cash transtion by the coffee name
    {$project: {
        coffee_name: "$_id",
        cash_order_share: {$cond: [{$gt: ["$total_orders",0]},{$divide: ["$cash_orders","$total_orders"]},null]},
        cash_revenue_share:{$cond: [{$gt: ["$total_rev",0]},{$divide: ["$cash_rev","$total_rev"]},null]},
        _id : 0
    }},
    {$sort: {cash_revenue_share : -1}}
]);
