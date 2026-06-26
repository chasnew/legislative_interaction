library(tidyverse)
library(cowplot)
library(lubridate)
library(lme4)
library(GGally)
library(scales)
library(brms)

li_path <- '/Users/chanuwasaswamenakul/Documents/workspace/legislative_interaction'
box_path <- '/Users/chanuwasaswamenakul/Library/CloudStorage/Box-Box'
model_path <- file.path(li_path, "full_model.rda")
legis_int_path <- file.path(box_path, 'LegislativeInteraction')
processed_path <- file.path(legis_int_path, 'processed_data')

first_congress_year <- 1789

# (co-)sponsorship data
complete_cosponsor <- read_csv(file.path(legis_int_path, 'processed_data', 'hr109_112_cosponsor.csv'))
unique_sponsor <- complete_cosponsor %>% 
  filter(sponsor) %>% 
  select(congress, bill_id, bioguide_id, name, party_code) %>% 
  rename(l_id = bioguide_id)

# house representative election data
house_election <- read_csv(file.path(legis_int_path, 'processed_data', 'house_election2004_2010.csv'))
trimmed_hr_election <- house_election %>% 
  select(congress, bioguide_id, general) %>% 
  rename(l_id = bioguide_id) %>% 
  mutate(e_challenge = 0.5 - general)

# congress age data
congress_age <- read_csv(file.path(legis_int_path, 'processed_data', 'congress_age_behav_data.csv'))
congress_age <- congress_age %>% 
  select(bioguide_id, first_congress) %>% 
  rename(l_id = bioguide_id)

# main sample of legislators and their earmark usage 2007-2009
data_path <- file.path(processed_path, 'house_yearmark_data2005_2012.csv')
stable_legis <- read_csv(data_path) %>% 
  mutate(total_num = solo_num + other_num)

# earmark usage and cross-partisan voting pre-earmark ban
data_path <- file.path(processed_path, 'indiv_yearmark_coop_preban.csv')
year_earmark_coop <- read_csv(data_path) %>% 
  mutate(year_congress = ((year - first_congress_year) / 2) + 1,
         party = fct_relevel(party,c("R","D")))


# individual voting data pre- and post- earmark ban
data_path <- file.path(processed_path, 'indiv_vote-x-earmark_data2005_2012.csv')
earmark_bill_vote <- read_csv(data_path)

# merge with congress age data
earmark_bill_vote <- earmark_bill_vote %>% 
  left_join(congress_age, by="l_id") %>% 
  mutate(year_congress = ((year - first_congress_year) / 2) + 1,
         congress_age = year_congress - first_congress,
         e_challenge = 1 - general,
         bipartisan = if_else(bill_party_count > 1, T, F))

xpartisan_vote <- earmark_bill_vote %>% 
  filter(party != bill_party) %>% 
  mutate(support = vote == "Yea",
         post = year >= 2011,
         earmark_class = case_when(total_pct < 0.2 ~ "1",
                                   total_pct >= 0.2 & total_pct < 0.4 ~ "2",
                                   total_pct >= 0.4 & total_pct < 0.6 ~ "3",
                                   total_pct >= 0.6 & total_pct < 0.8 ~ "4",
                                   .default = "5"),
         # earmark_use = ifelse(total_pct < 0.5, "low", "high"),
         major_house = case_when(
           year %in% c(2005, 2006, 2011, 2012) ~ "R",
           year %in% 2007:2010 ~ "D",
           .default = NA
         ),
         is_major = (major_house == party))



##################### Theory Diagram: Figure 0 ########################
theory_diagram_file <- file.path(li_path, "img", "earmark_theory_diagram.jpg")
theory_diagram <- ggdraw() +
  draw_image(
    theory_diagram_file, scale = 1
  )

etl_diagram_file <- file.path(li_path, "img", "earmark_timeline.jpg")
e_timeline <- ggdraw() +
  draw_image(
    etl_diagram_file, scale = 1
  )

fig1_alabel <- ggdraw() + 
  draw_label(
    "a",
    fontface = 'bold',
    size = 18,
    x = 0, y = 0.1,
    hjust = 0, vjust = 0
  )

fig1_blabel <- ggdraw() + 
  draw_label(
    "b",
    fontface = 'bold',
    size = 18,
    x = 0, y = 0.1,
    hjust = 0, vjust = 0
  )

theory_figure0 <- plot_grid(
  fig1_alabel, theory_diagram,
  fig1_blabel, e_timeline,
  ncol = 1, rel_heights = c(0.1, 1, 0.1, 0.3)
)

ggsave(file.path(li_path, "img", "main_theory0.jpg"), 
       plot = theory_figure0, height = 10, width = 8)




##################### MAIN RESULT: Figure 1 ########################
# Earmark request numbers aggregate by party from 2007-2009
earmark_agg <- stable_legis %>% 
  group_by(party, cat_year) %>% 
  summarize(total_num.sum = sum(total_num),
            total_num.avg = mean(total_num),
            total_num.sd = sd(total_num),
            legis_n = n(),
            total_num.se = total_num.sd / sqrt(legis_n))
earmark_agg$party <- fct_relevel(as.factor(earmark_agg$party), c("R", "D"))

# yearly earmark group by house majority / minority
yearly_earmark_plot <- earmark_agg %>%
  ggplot(aes(x=cat_year, y=total_num.avg, color=party)) +
  geom_point(alpha=0.5, size=2.5) +
  geom_errorbar(aes(ymin=total_num.avg-(2*total_num.se),
                    ymax=total_num.avg+(2*total_num.se)),
                width=0.2) +
  geom_line() +
  scale_x_continuous("Year", breaks = 2007:2009) +
  scale_y_continuous("Legislator's\nEarmark Usage", limits=c(10,30)) +
  scale_color_manual(values = c("red", "blue"),
                     labels = c("Republican", "Democrat")) +
  theme_classic() +
  theme(axis.title=element_text(size=14,face="bold"),
        axis.text=element_text(size=14),
        legend.title=element_blank(),
        legend.text = element_text(size=14),
        legend.direction = "horizontal")

# extract shared legend from the first plot
shared_legend <- get_legend(yearly_earmark_plot)
yearly_earmark_plot <- yearly_earmark_plot + theme(legend.position = "none")


# earmark use associated with cross-partisan voting
legis_earmark_coop <- year_earmark_coop %>% 
  group_by(l_id) %>% 
  summarize(sum_total = sum(total_num), coop = mean(coop), party = first(party))

agg_ecoop_plot <- legis_earmark_coop %>% 
  ggplot(aes(x=sum_total, y=coop)) +
  geom_point(alpha=0.8) +
  scale_x_continuous("Total legislator's earmark use") +
  scale_y_continuous("Legislator cross party\nvoting rate") +
  geom_smooth(method="lm", formula=y~x) +
  theme_classic() +
  theme(axis.title=element_text(size=14,face="bold"),
        axis.text=element_text(size=14),
        legend.position="None")

# agg_party_ecoop_plot <- legis_earmark_coop %>%
#   ggplot(aes(x=sum_total, y=coop, color=party)) +
#   geom_point(alpha=0.5) +
#   scale_x_continuous("Total legislator's earmark usage") +
#   scale_y_continuous("Legislator cross party\nvoting rate", limits = c(0.2, 1.1)) +
#   scale_color_manual(values = c("red", "blue"),
#                      labels = c("Republican", "Democrat")) +
#   geom_smooth(method="lm", formula=y~x) +
#   guides(color=guide_legend(title="Party")) +
#   theme_classic() +
#   theme(axis.title=element_text(size=14,face="bold"),
#         axis.text=element_text(size=14),
#         legend.position="None")

dem_ecoop_plot <- legis_earmark_coop %>% 
  filter(party == 'D') %>% 
  ggplot(aes(x=sum_total, y=coop)) +
  geom_point(alpha=0.5, color="blue") +
  scale_x_continuous("Total legislator's earmark use") +
  scale_y_continuous("Legislator cross party\nvoting rate") +
  geom_smooth(method="lm", formula=y~x) +
  theme_classic() +
  theme(axis.title=element_text(size=14,face="bold"),
        axis.text=element_text(size=14),
        legend.position="None")

rep_ecoop_plot <- legis_earmark_coop %>% 
  filter(party == 'R') %>% 
  ggplot(aes(x=sum_total, y=coop)) +
  geom_point(alpha=0.5, color="red") +
  scale_x_continuous("Total legislator's earmark use") +
  scale_y_continuous("Legislator cross party\nvoting rate") +
  geom_smooth(method="lm", formula=y~x, color="red") +
  theme_classic() +
  theme(axis.title=element_text(size=14,face="bold"),
        axis.title.y=element_blank(),
        axis.text=element_text(size=14),
        legend.position="None")

party_ecoop_plot <- plot_grid(dem_ecoop_plot, rep_ecoop_plot,
                              ncol=2, rel_widths=c(1,0.9))

# average cross-partisan support (major house vs minor house)
# plot_colors <- hue_pal()(2)
xpartisan_agg <- xpartisan_vote %>% 
  group_by(year, vote_id, is_major) %>% 
  summarize(support.avg = mean(support)) %>% 
  group_by(year, is_major) %>% 
  summarize(coop = mean(support.avg), vote_n = n(),
            coop.sd = sd(support.avg), coop.se = coop.sd / sqrt(vote_n)) %>% 
  mutate(mm_party = as.factor(if_else(is_major, "major", "minority"))) %>% 
  ungroup()

yearly_coop_plot <- xpartisan_agg %>% 
  mutate(major_house = if_else(((year %in% c(2005, 2006, 2011, 2012) & is_major) |
                                  (year %in% (2007:2010) & !is_major)), "R", "D"),
         coop.min = coop-(2*coop.se),
         coop.max = pmin(1, coop+(2*coop.se))) %>% 
  ggplot(aes(x=year, y=coop, group=mm_party, color=major_house)) +
  geom_point(aes(shape=mm_party), size=2.5) +
  geom_errorbar(aes(ymin=coop.min,
                    ymax=coop.max),
                width=0.2) +
  geom_line() +
  geom_vline(xintercept = 2007, linetype = "dashed") +
  geom_vline(xintercept = 2009.8, linetype = "dashed") +
  geom_vline(xintercept = 2010.8, linetype = "dashed") +
  annotate('rect', xmin=2007, xmax=2009.8, ymin=-Inf, ymax=Inf, alpha=.2, fill='black') +
  annotate(geom="text", x=2008.4, y=0.75, label="Earmark data available",
           color="black", size=5) +
  annotate(geom="text", x=2011.4, y=0.7, label="Earmark\nban",
           color="black", size=5) +
  scale_y_continuous("Legislator cross party\nvoting rate", limits = c(0.4,1)) +
  scale_color_manual(values = c("blue", "red"),
                     labels = c("Republican", "Democrat")) +
  scale_shape_discrete(labels=c("House Majority", "House Minority")) +
  guides(color = FALSE) +
  theme_classic() +
  theme(axis.title=element_text(size=14,face="bold"),
        axis.text=element_text(size=14),
        legend.title=element_blank(),
        legend.text=element_text(size=14),
        legend.position=c(0.15,0.12)
        )

# ggsave(file.path(li_path, "img", "yearly_coop_demrep.png"),
#        plot = yearly_coop_plot, height = 6, width = 12)
# yearly_coop_plot <- yearly_coop_plot + theme(legend.position = "none")


# MAIN RESULT 1 figure

fig1_alabel <- ggdraw() + 
  draw_label(
    "a",
    fontface = 'bold',
    size = 16,
    x = 0, y = 0.1,
    hjust = 0, vjust = 0
  )

fig1_blabel <- ggdraw() + 
  draw_label(
    "b",
    fontface = 'bold',
    size = 16,
    x = 0, y = 0.1,
    hjust = 0, vjust = 0
  )


fig1_clabel <- ggdraw() + 
  draw_label(
    "c",
    fontface = 'bold',
    size = 16,
    x = 0, y = 0.1,
    hjust = 0, vjust = 0
  )

fig1_dlabel <- ggdraw() + 
  draw_label(
    "d",
    fontface = 'bold',
    size = 16,
    x = 0, y = 0.1,
    hjust = 0, vjust = 0
  )


time_trend_grid <- plot_grid(fig1_alabel, fig1_blabel,
                             yearly_coop_plot, yearly_earmark_plot,
                             ncol=2, rel_widths=c(1,0.7), rel_heights=c(0.1,1))

ecoop_grid <- plot_grid(fig1_clabel, fig1_dlabel,
                        agg_ecoop_plot, party_ecoop_plot,
                        ncol=2, rel_widths=c(1,1), rel_heights=c(0.1,1))

main_plot1 <- plot_grid(time_trend_grid, ecoop_grid, ncol = 1, rel_heights=c(1,1))

main_fig1 <- plot_grid(shared_legend, main_plot1,
                       ncol = 1, rel_heights = c(0.07, 1))

ggsave(file.path(li_path, "img", "main_result1_1.jpg"), 
       plot = main_fig1, height = 8, width = 14)



##################### MAIN RESULT: Figure 2 ########################
# select top and bottom earmark users in equal numbers from each party
unique_main_legis <- xpartisan_vote %>% 
  select(l_id, party, party_total_pct) %>% 
  filter(!duplicated(l_id))

top_dem <- unique_main_legis %>% 
  filter(party == "D") %>% 
  arrange(desc(party_total_pct)) %>% 
  slice_head(n=40) %>% 
  mutate(earmark_use = 'high')

btm_dem <- unique_main_legis %>% 
  filter(party == "D") %>% 
  arrange(desc(party_total_pct)) %>% 
  slice_tail(n=40) %>% 
  mutate(earmark_use = 'low')

top_rep <- unique_main_legis %>% 
  filter(party == "R") %>% 
  arrange(desc(party_total_pct)) %>% 
  slice_head(n=40) %>% 
  mutate(earmark_use = 'high')

btm_rep <- unique_main_legis %>% 
  filter(party == "R") %>% 
  arrange(desc(party_total_pct)) %>% 
  slice_tail(n=40) %>% 
  mutate(earmark_use = 'low')

balanced_legis <- rbind(top_dem, btm_dem, top_rep, btm_rep) %>% 
  select(l_id, earmark_use)

# voting data with balanced sample of top and bottom earmark users
balanced_xvote <- xpartisan_vote %>% 
  inner_join(balanced_legis, by='l_id')

# changes by year
agg_yearmark_coop <- balanced_xvote %>%
  group_by(earmark_use, vote_id, year) %>% 
  summarize(support.avg = mean(support)) %>% 
  group_by(earmark_use, year) %>% 
  summarize(coop = mean(support.avg), vote_n = n(),
            coop.sd = sd(support.avg), coop.se = coop.sd / sqrt(vote_n))


year_annotates <- tibble(year=c(2006, 2008, 2010, 2011.8), y=c(0.75, 0.75, 0.75, 0.75),
                         text=c("109th congress",
                                "110th congress",
                                "111th congress &\nObama's 1st term", 
                                "112th congress &\nearmark ban"))

agg_yearmark_coop %>%
  ggplot(aes(x=year, y=coop, color=factor(earmark_use), group=earmark_use)) +
  geom_line() +
  geom_point(size=2.5) +
  geom_errorbar(aes(ymin=coop-(2*coop.se),
                    ymax=coop+(2*coop.se)),
                width=0.1) +
  scale_x_continuous("Year") +
  scale_y_continuous("Cross-partisan voting rate") +
  # geom_text(data=year_annotates, aes(x=year, y=y, label=text, group=NA),
  #           color="black", size=4) +
  annotate('rect', xmin=2007, xmax=2009.8, ymin=-Inf, ymax=Inf, alpha=.2, fill='black') +
  annotate(geom="text", x=2008.4, y=0.5, label="Earmark data available",
           color="black", size=5) +
  annotate(geom="text", x=2011.2, y=0.7, label="Earmark\nban",
           color="black", size=5) +
  geom_vline(xintercept = c(2007, 2009.8, 2010.8), linetype = "dashed") +
  guides(color=guide_legend(title="Earmark use")) +
  theme_classic()

# ggsave(file.path(li_path, "img", "yearly_earmark_coop.png"), height = 6, width = 12)


# changes between pre- and post- earmark ban
agg_prepost_coop <- balanced_xvote %>% 
  group_by(earmark_use, vote_id, post) %>% 
  summarize(support.avg = mean(support)) %>% 
  group_by(earmark_use, post) %>% 
  summarize(coop = mean(support.avg), vote_n = n(),
            coop.sd = sd(support.avg), coop.se = coop.sd / sqrt(vote_n)) %>% 
  mutate(post = if_else(post, "post", "pre"),
         post = fct_relevel(post,c("pre","post")))

did_topbot40 <- agg_prepost_coop %>% 
  ggplot(aes(x=post, y=coop, color=factor(earmark_use), group=earmark_use)) +
  geom_line() +
  geom_point(size=2.5) +
  geom_errorbar(aes(ymin=coop-(2*coop.se),
                    ymax=coop+(2*coop.se)),
                width=0.1) +
  scale_x_discrete("Pre-/Post-Earmark ban") +
  scale_y_continuous("Cross-partisan voting rate") +
  guides(color=guide_legend(title="Earmark usage")) +
  theme_classic() +
  theme(axis.title=element_text(size=16,face="bold"),
        axis.text=element_text(size=14),
        legend.title=element_text(size=14),
        legend.text = element_text(size=12),
        legend.position=c(0.75, 0.85))
# ggsave(file.path(li_path, "img", "prepost_earmark_coop.png"), height = 6, width = 12)


# changes between pre- and post- earmark ban *BY PARTY*
party_prepost_coop <- balanced_xvote %>% 
  group_by(earmark_use, vote_id, party, post) %>% 
  summarize(support.avg = mean(support)) %>% 
  group_by(earmark_use, party, post) %>% 
  summarize(coop = mean(support.avg), vote_n = n(),
            coop.sd = sd(support.avg), coop.se = coop.sd / sqrt(vote_n)) %>% 
  mutate(post = if_else(post, "post", "pre"),
         post = fct_relevel(post,c("pre","post")))

dem_prepost <- party_prepost_coop %>% 
  filter(party == 'D') %>% 
  ggplot(aes(x=post, y=coop, color=factor(earmark_use), group=earmark_use)) +
  geom_line() +
  geom_point(size=2.5) +
  geom_errorbar(aes(ymin=coop-(2*coop.se),
                    ymax=coop+(2*coop.se)),
                width=0.1) +
  scale_x_discrete("Pre-/Post-Earmark ban") +
  scale_y_continuous("Cross-partisan voting rate") +
  ggtitle("Democrats") +
  guides(color=guide_legend(title="Earmark usage")) +
  theme_classic() +
  theme(plot.title=element_text(size=16,face="bold",hjust=0.5),
        axis.title=element_text(size=14,face="bold"),
        axis.text=element_text(size=14),
        legend.title=element_text(size=14),
        legend.text=element_text(size=12),
        legend.position=c(0.75, 0.8))

rep_prepost <- party_prepost_coop %>% 
  filter(party == 'R') %>% 
  ggplot(aes(x=post, y=coop, color=factor(earmark_use), group=earmark_use)) +
  geom_line() +
  geom_point(size=2.5) +
  geom_errorbar(aes(ymin=coop-(2*coop.se),
                    ymax=coop+(2*coop.se)),
                width=0.1) +
  scale_x_discrete("Pre-/Post-Earmark ban") +
  scale_y_continuous("Cross-partisan voting rate") +
  guides(color=guide_legend(title="Earmark usage")) +
  ggtitle("Republicans") +
  theme_classic() +
  theme(plot.title=element_text(size=16,face="bold",hjust=0.5),
        axis.title=element_text(size=14,face="bold"),
        axis.text=element_text(size=14),
        legend.position="None")

did_topbot40_by_party <- plot_grid(dem_prepost, rep_prepost,
                                   ncol=2, rel_widths=c(1,1))





load(model_path)
summary(full.model)
# Counter-factual predictions
xpartisan_vote <- xpartisan_vote %>% 
  mutate(post = year >= 2011,
         t_to_post = as.integer(date - ymd('2011-01-01')),
         norm_t = (t_to_post - min(t_to_post)) / (max(t_to_post) - min(t_to_post)),
         solo_num.z = scale(solo_num)[,1],
         total_num.z = scale(total_num)[,1],
         solo_amount.zlog = scale(log10(solo_amount + 1))[,1],
         total_amount.zlog = scale(log10(total_amount + 1))[,1],
         congress_age.z = scale(congress_age)[,1],
         e_challenge.z = scale(e_challenge)[,1])

train_data <- xpartisan_vote %>% 
  select(vote_id, congress_age.z, e_challenge.z, is_major, total_num.z, post, year, norm_t, l_id, support)

counterf_data <- train_data %>% 
  filter(year >= 2010) %>% 
  mutate(post=F)

train_data$prob.pred <- predict(full.model, newdata=train_data, type="response")
counterf_data$prob.pred <- predict(full.model, newdata=counterf_data, type="response")

counterf.predictions <- counterf_data %>% 
  group_by(l_id, year) %>% 
  summarize(countrf.pred = mean(prob.pred))

agg.predictions <- train_data %>% 
  group_by(l_id, year) %>% 
  summarize(real = mean(support), pred = mean(prob.pred)) %>% 
  left_join(counterf.predictions, by = join_by(l_id, year)) %>% 
  pivot_longer(cols = c(real, pred, countrf.pred),
               names_to = "coop.type",
               values_to = "coop.val") %>% 
  filter(!is.na(coop.val))


year_annotates <- tibble(year=c(2011.2), y=c(0.9),
                         text=c("earmark ban"))

ban_line <- ggplot() +
  annotate("segment", y=0,yend=3,x=0,xend=0, linewidth=1.5, linetype="dashed") +
  annotate("text", x=0, y=3.2, angle=0,
           label = "earmark ban",
           size = 6.2) +
  scale_x_continuous(limits = c(-0.1, 0.1), expand = c(0,0)) +
  scale_y_continuous(limits = c(0, 3.5)) +
  theme_classic() +
  theme(axis.line = element_blank(),
        axis.title = element_blank(),
        axis.ticks = element_blank(),
        axis.text = element_blank())
# ban_line

counterf_pred_plot <- agg.predictions %>% 
  group_by(year, coop.type) %>% 
  summarize(coop = mean(coop.val)) %>% 
  ggplot(aes(x=year, y=coop, color=coop.type)) +
  geom_point() +
  geom_line() +
  annotate("segment", y=0.67,yend=0.8,x=2010.8,xend=2010.8, linewidth=1, linetype="dashed") +
  annotate("text", x=2010.8, y=0.805, angle=0,
           label = "earmark ban",
           size = 5) +
  scale_y_continuous("Cross-partisan voting rate", expand = c(0.02,0)) +
  scale_color_manual(labels=c("Counterfactual", "Prediction", "Data"),
                     values=c("#56B4E9", "#E69F00", "#999999")) +
  theme_classic() +
  theme(axis.title=element_text(size=16,face="bold"),
        axis.text=element_text(size=14),
        legend.title=element_blank(),
        legend.text=element_text(size=14),
        legend.position=c(0.5,0.2))




fig1_alabel <- ggdraw() + 
  draw_label(
    "a",
    fontface = 'bold',
    size = 16,
    x = 0, y = 0.1,
    hjust = 0, vjust = 0
  )

fig1_blabel <- ggdraw() + 
  draw_label(
    "b",
    fontface = 'bold',
    size = 16,
    x = 0, y = 0.1,
    hjust = 0, vjust = 0
  )


plot_grid(fig1_alabel, NA, fig1_blabel,
          did_topbot40, NA, counterf_pred_plot,
          ncol = 3, rel_widths = c(1, 0.05, 1),
          rel_heights = c(0.1, 1))

ggsave(file.path(li_path, "img", "main_result2.jpg"), height = 6, width = 12)
