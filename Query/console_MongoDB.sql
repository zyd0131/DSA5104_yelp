// 1
db.visit.aggregate([
    {
        $group: {
            _id: { user_id: "$user_id", business_id: "$business_id" },
            visit_count: { $sum: 1 }
        }
    },
    {
        $match: {
            visit_count: { $gte: 5 }
        }
    },
    {
        $group: {
            _id: "$_id.business_id",
            user_count: { $sum: 1 }
        }
    },
    {
        $match: {
            user_count: { $gte: 1 }
        }
    },
    {
        $lookup: {
            from: "business",
            localField: "_id",
            foreignField: "_id",
            as: "business_info"
        }
    },
    {
        $unwind: "$business_info"
    },
    {
        $match: {
            "business_info.state": "NV"
        }
    },
    {
        $project: {
            _id: "$business_info._id",
            name: "$business_info.name"
        }
    }
]);


// 2
db.business.aggregate([
    {
        $unwind: "$categories"
    },
    {
        $group: {
            _id: "$categories",
            business_count: { $sum: 1 },
            businesses: {
                $push: {
                    business_id: "$_id",
                    name: "$name",
                    stars: "$stars"
                }
            }
        }
    },
    {
        $sort: { "business_count": -1 }
    },
    {
        $limit: 1
    },
    {
        $unwind: "$businesses"
    },
    {
        $match: {
            "businesses.stars": 5
        }
    },
    {
        $project: {
            _id: 0,
            category: "$_id",
            business_id: "$businesses.business_id",
            name: "$businesses.name"
        }
    }
])



//3
db.business.createIndex({ stars: 1, city: 1 });
db.review.createIndex({ business_id: 1 });

db.business.aggregate([
  {
    $match: {
      city: 'Santa Barbara',
      stars: { $gte: 4 },
    },
  },
  {
    $lookup: {
      from: 'review',
      localField: '_id',
      foreignField: 'business_id',
      as: 'reviews',
    },
  },
  {
    $unwind: '$reviews',
  },
  {
    $group: {
      _id: {
        business_id: '$_id',
        name: '$name',
        address: '$address',
        city: '$city',
        state: '$state',
        postal_code: '$postal_code',
        latitude: '$latitude',
        longitude: '$longitude',
        stars: '$stars',
        is_open: '$is_open',
      },
      avg_stars: { $avg: '$reviews.stars' },
      total_reviews: { $sum: 1 },
      total_useful: { $sum: '$reviews.useful' },
    },
  },
  {
    $project: {
      name: '$_id.name',
      address: '$_id.address',
      city: '$_id.city',
      _id: 0,
      avg_stars: 1,
      total_reviews: 1,
      total_useful: 1,
    },
  },
  {
    $match:{
        $expr:{
               $gte:["$avg_stars","$stars"],
        }
    }
  },
]);

db.business.createIndex({ stars: 1, city: 1 });
db.review.createIndex({ business_id: 1 });
db.business.aggregate([
  {
    $match: {
      city: 'Santa Barbara',
      stars: { $gt: 4 },
    },
  },
  {
    $lookup: {
      from: 'review',
      localField: '_id',
      foreignField: 'business_id',
      as: 'reviews',
    },
  },
  {
    $addFields: {
      filteredReviews: {
        $filter: {
          input: '$reviews',
          as: 'review',
          cond: {
            $gt: ['$$review.date', new Date('2020-01-01T00:00:00')],
          },
        },
      },
    },
  },
  {
        $addFields:{
              avgBusinessStars: {
            $avg: '$filteredReviews.stars',
          },
        }
  },
  {
    $match: {
      $expr: {
        $gt: ["$avgBusinessStars", "$stars"],
      },
    },
  },
  {
    $project: {
      name: 1,
      address: 1,
      city: 1,
      state: 1,
      stars: 1,
      avgBusinessStars: 1,
//      filteredReviews: 1,
      _id: 0,
    },
  },
]);

// 4
db.business.createIndex({ business_id: 1 });
db.user.createIndex({ friends: 1 });
db.visit.createIndex({ user_id: 1 });
db.visit.createIndex({ business_id: 1 });
db.user.createIndex({ name: 1 });

db.user.aggregate([
    { $match: { name: "John" } },
    {
        $lookup: {
            from: "visit",
            localField: "_id",
            foreignField: "user_id",
            as: "userVisit",
        },
    },
    {
        $lookup:{
            from: "business",
            localField: "userVisit.business_id",
            foreignField: "_id",
            as: "businessUser",
        }
    },
    {
           $match: {
            businessUser: {
                $elemMatch: {
                    categories: "Shopping"
                }
            }
        }
    },

    { $addFields: { userShoppingCount: { $size: "$businessUser" } } },
    { $unwind: "$friends" },
    {
        $lookup: {
            from: "visit",
            localField: "friends",
            foreignField: "user_id",
            as: "userFriendVisit",
        },
    },
        {
        $lookup:{
            from: "business",
            localField: "userFriendVisit.business_id",
            foreignField: "_id",
            as: "businessUserFriend",
        }
    },

    {
           $match: {
            businessUserFriend: {
                $elemMatch: {
                    categories: "Shopping"
                }
            }
        }
    },

    { $addFields: { userFriendShoppingCount: { $size: "$businessUserFriend" } } },
    { $lookup: { from: "user", localField: "friends", foreignField: "_id", as: "friendInfo" } },
    { $unwind: "$friendInfo" },
   {
        $match: {
          $expr: {
            $gt: ["$userFriendShoppingCount", "$userShoppingCount"]
          }
        }
   },
    {
        $project: {
            _id: 0,
            user_name: "$friendInfo.name", //
//            yelping_since: "$yelping_since", //
            review_count: "$review_count"
        }
    }
]);

// 5
db.user.aggregate([
  {
    $addFields: {
      influence_score: {
        $sum: [
          "$compliment_hot",
          "$compliment_more",
          "$compliment_profile",
          "$compliment_cute",
          "$compliment_list",
          "$compliment_note",
          "$compliment_plain",
          "$compliment_cool",
          "$compliment_funny",
          "$compliment_writer",
          "$compliment_photos",
          {$multiply: [{$size: "$elite"}, 100]}
        ]
      }
    }
  },
  {
    $project: {
      _id: 1,
      influence_score: 1
    }
  }
])

// 6
db.business.aggregate([
  {
    $addFields: {
      count_2019: {
        $size: {
          $filter: {
            input: { $ifNull: ["$checkin_date", []] },
            as: "date",
            cond: { $eq: [{ $year: "$$date" }, 2019] }
          }
        }
      },
      count_2020: {
        $size: {
          $filter: {
            input: { $ifNull: ["$checkin_date", []] },
            as: "date",
            cond: { $eq: [{ $year: "$$date" }, 2020] }
          }
        }
      }
    }
  },
  {
    $match: {
      $expr: {
        $gt: ["$count_2020", "$count_2019"]
      }
    }
  },
  {
    $project: {
      name: 1
    }
  }
])

// 7
db.review.aggregate(
    [
        {
            $lookup: {
                from: "business",
                localField: "business_id",
                foreignField: "_id",
                as: "business"
            }
        },
        {
            $unwind: "$business"
        },
        {
            $match: {
                "business.city": "Santa Barbara",
                "date": { $gt: ISODate("2023-01-01T00:00:00.000Z") }
            }
        },
        {
            $group: {
                _id:{business_id:"$business_id",name:"$business.name"},
                avg_star: { $avg: "$stars" }
            }
        },
        {
            $match: {
                "avg_star": { $gt: 4 }
            }
        },
        {
            $sort: {
                avg_star: -1
            }
        },
        {
            $project: {
                business_id: "$_id.business_id",
                name: "$_id.name",
                avg_star: "$avg_star",
	            _id:0
            }
        }
    ]
)

// 8
db.review.aggregate(
    [
        {
            $lookup: {
                from: "business",
                localField: "business_id",
                foreignField: "_id",
                as: "business"
            }
        },
        {
            $unwind: "$business"
        },
        {
            $match: {
                text: { $regex: "excellent service"}
            }
        },
        {
            $project: {
                _id: 0,
                business_id: '$business_id',
                name: "$business.name",
                text: '$text'
            }
        }
    ]
)

// 9
db.business.aggregate(
    [
        {
            $match: {
                "stars": 5
            }
        },
        {
            $group: {
                _id: "$city",
                count: { $sum: 1 }
            }
        },
        {
            $sort: {
                count: -1
            }
        },
        {
            $limit: 1
        }
    ]
)

// 10
db.business.aggregate([
    {
        $match: {
            city: "Whitestown"
        }
    },
    {
        $lookup: {
            from: "review",
            localField: "_id",
            foreignField: "business_id",
            as: "review_info"
        }
    },
    {
        $unwind: "$review_info"
    },
    {
        $lookup: {
            from: "user",
            localField: "review_info.user_id",
            foreignField: "_id",
            as: "user_info"
        }
    },
    {
        $unwind: "$user_info"
    },
{
    $project: {
        _id: 0,
        user_id: "$user_info._id", // 确保这里引用的是users集合中的用户ID
        business_id: "$_id", // businesses集合的ID应该直接从当前文档中获取
        stars: "$review_info.stars" // 星级应从reviews集合获取
    }
}
])

// 11
db.business.aggregate([
 {
 $match: {
 is_open: 1
 }
 },
 {
 $unwind: "$categories"
 },
 {
 $group: {
 _id: { city: "$city", category: "$categories" },
 averageStars: { $avg: "$stars" },
 businessCount: { $sum: 1 }
 }
 },
{
 $match: {
 businessCount: { $gt: 10 }
 }
 },
 {
 $sort: { "businessCount": -1 }
 },
 {
 $group: {
 _id: "$_id.city",
 topCategory: { $first: "$_id.category" },
 averageStars: { $first: "$averageStars" },
 businessCount: { $first: "$businessCount" }
 }
 },
 {
 $project: {
 _id: 0,
 city: "$_id",
 category: "$topCategory",
 averageStars: 1,
 businessCount: 1
 }
 }
]);


// 12
db.business.find().forEach(function(doc) {
  db. business.updateOne(
    { _id: doc._id },
    {
      $set: {
        location: {
          type: "Point",
          coordinates: [doc.longitude, doc.latitude]
        }
      }
    }
  );
});

db.business.createIndex({ "location": "2dsphere" })

db.business.find({
  city: "Tucson",
  stars: { $gt: 4 },
  is_open: 1,
  location: {
    $geoWithin: {
      $centerSphere: [[-110.91179, 32.25346], 50 / 6378.1]
    }
  }
}, {
  name: 1,
  address: 1,
  city: 1,
  postal_code: 1,
  stars: 1,
  _id: 0
}).sort({ stars: -1 });


